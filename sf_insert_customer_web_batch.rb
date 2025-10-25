# frozen_string_literal: true
require "fileutils"
require "logger"
require "open3"
require "shellwords"
require "csv"
require "parallel"

class SfInsertCustomerWebBatch < BatchBase
  DEFAULT_LIMIT = 100
  CLI_PATH = "./cli-kintone"
  LOG_DIR = Rails.root.join("log", "batches")

  def run # rubocop:disable Metrics/MethodLength
    initialize_logger

    begin
      @logger.info "=== SfInsertCustomerWebBatch 開始 ==="
      @logger.info "開始時刻: #{Time.current.strftime("%Y-%m-%d %H:%M:%S")}"

      limit = ENV.fetch("SALESFORCE_CUSTOMER_WEB_LIMIT", nil) ? ENV["SALESFORCE_CUSTOMER_WEB_LIMIT"].to_i : DEFAULT_LIMIT
      sf_customer_webs = SfCustomerWeb.unsent_list[0, limit]
      if sf_customer_webs.blank?
        @logger.error "対応データが'0'件です。処理を中止しました。"
        return
      end

      place_kintone_client = KintoneClient.new(Settings.kintone_apps.subdomain, Settings.kintone_apps.place.api_token)
      customer_kintone_client = KintoneClient.new(Settings.kintone_apps.subdomain, Settings.kintone_apps.customer.api_token)
      place_introduce_kintone_client = KintoneClient.new(Settings.kintone_apps.subdomain, Settings.kintone_apps.place_introduce.api_token)

      place_records = place_kintone_client.get_records(Settings.kintone_apps.place.app_id)
      customer_records = customer_kintone_client.get_records(Settings.kintone_apps.customer.app_id)
      place_introduce_records = place_introduce_kintone_client.get_records(Settings.kintone_apps.place_introduce.app_id)
      @logger.info "place_introduce_records：#{place_introduce_records}"

      max_pi_number = find_max_pi_number(place_introduce_records)
      @logger.info "式場紹介現在の最大PI番号: PI-#{max_pi_number.to_s.rjust(8, "0")}"

      after_process_sf_customer_webs = []
      place_introduce_list = []

      sf_customer_webs.each do |sf_customer_web|
        process_sf_customer_web(sf_customer_web, place_records, customer_records, place_introduce_records, max_pi_number, place_introduce_list, after_process_sf_customer_webs)
      end

      @logger.info "処理完了: Web予約 #{after_process_sf_customer_webs.length}件, 式場紹介 #{place_introduce_list.length}件"

      process_customer_web_data(after_process_sf_customer_webs)
      process_place_introduce_data(place_introduce_list)

      @logger.info "終了時刻: #{Time.current.strftime("%Y-%m-%d %H:%M:%S")}"
      @logger.info "=== SfInsertCustomerWebBatch 終了 ==="
    ensure
      @logger.info("処理終了")
    end
  end

  private

  def process_customer_web_data(after_process_sf_customer_webs)
    if after_process_sf_customer_webs.any?
      sf_customer_web_csv_path = SfCustomerWeb.export_to_csv(after_process_sf_customer_webs, "sf_customer_web")
      import_to_kintone_parallel(
        sf_customer_web_csv_path,
        Settings.kintone_apps.customer.app_id,
        id_mode: :customer,
        batch_size: 100,
        threads: 4
      )
    else
      @logger.info "Web予約の処理データが0件のため、CSVエクスポートをスキップしました。"
    end
  end

  def process_place_introduce_data(place_introduce_list)
    if place_introduce_list.any?
      place_introduce_csv_path = SfCustomerWeb.export_to_csv(place_introduce_list, "place_introduce")
      import_to_kintone_parallel(
        place_introduce_csv_path,
        Settings.kintone_apps.place_introduce.app_id,
        id_mode: :place_introduce,
        batch_size: 100,
        threads: 4
      )
    else
      @logger.info "式場紹介の処理データが0件のため、CSVエクスポートをスキップしました。"
    end
  end

  def import_to_kintone_parallel(csv_path, app_id, id_mode:, batch_size: 100, threads: 4)
    unless File.exist?(csv_path)
      @logger.warn "CSVファイルが存在しません: #{csv_path}"
      return { success: false, message: "CSVファイルが存在しません: #{csv_path}" }
    end

    kintone_url = Settings.kintone_apps.kintone_url
    csv_encode = Settings.kintone_apps.csv_encode
    user_name = Settings.kintone_apps.user_name
    password = Settings.kintone_apps.password

    records = []
    CSV.foreach(csv_path, headers: true, encoding: "Shift_JIS") do |row|
      records << row
    end

    total_count = records.size
    headers = total_count > 0 ? records[0].headers : []

    @logger.info "並列インポート開始: #{csv_path} (全 #{total_count} 行, スレッド: #{threads}, バッチサイズ: #{batch_size})"

    batches = records.each_slice(batch_size).to_a

    results = Parallel.map_with_index(batches, in_threads: threads) do |batch, batch_index|
      import_batch_optimized_customer_web(
        batch,
        headers,
        kintone_url,
        app_id,
        user_name,
        password,
        csv_encode,
        batch_index + 1,
        id_mode
      )
    end

    import_result = results.each_with_object({
      success: false,
      total: total_count,
      success_count: 0,
      error_count: 0,
      errors: [],
      success_ids: [],
      failed_ids: []
    }) do |result, sum|
      sum[:success_count] += result[:success_count]
      sum[:error_count] += result[:error_count]
      sum[:success_ids] += result[:success_ids]
      sum[:failed_ids] += result[:failed_ids]
      sum[:errors] += result[:errors]
    end

    import_result[:success] = import_result[:error_count] == 0
    log_final_result_with_ids(import_result, csv_path, app_id)
    import_result
  rescue => e
    error_result = {
      success: false,
      total: 0,
      success_count: 0,
      error_count: 0,
      errors: ["システムエラー: #{e.message}"],
      success_ids: [],
      failed_ids: []
    }
    @logger.error "インポート処理中に例外が発生しました: #{e.message}"
    @logger.error e.backtrace.join("\n")
    error_result
  end

  def import_batch_optimized_customer_web(batch, headers, kintone_url, app_id, user_name, password, csv_encode, batch_number, id_mode)
    batch_result = {
      success_count: 0,
      error_count: 0,
      success_ids: [],
      failed_ids: [],
      errors: []
    }

    batch_success = import_batch_direct_customer_web(batch, headers, kintone_url, app_id, user_name, password, csv_encode, batch_number)

    if batch_success[:success]
      batch_result[:success_count] = batch.size
      batch_result[:success_ids] = batch.map { |r| extract_record_id_for_mode(r, id_mode) }
      @logger.info "バッチ #{batch_number} 一括インポート成功: #{batch.size} レコード"
    else
      @logger.warn "バッチ #{batch_number} 一括インポート失敗、単一レコード処理に切り替え: #{batch_success[:error]}"
      process_batch_records_individually(batch, headers, kintone_url, app_id, user_name, password, csv_encode, id_mode, batch_result)
    end

    batch_result
  end

  def process_batch_records_individually(batch, headers, kintone_url, app_id, user_name, password, csv_encode, id_mode, batch_result)
    batch.each do |record|
      id_value = extract_record_id_for_mode(record, id_mode)
      single_result = import_single_record_fast_customer_web(
        record, headers, kintone_url, app_id, user_name, password, csv_encode
      )
      if single_result[:success]
        batch_result[:success_count] += 1
        batch_result[:success_ids] << id_value
      else
        batch_result[:error_count] += 1
        batch_result[:failed_ids] << id_value
        id_label = case id_mode
                   when :customer then "minfamiActionType + actionId"
                   when :place_introduce then "Name"
                   else "ID"
                   end
        batch_result[:errors] << "レコード (#{id_label}: #{id_value}): #{single_result[:error]}"
      end
    end
  end

  def import_batch_direct_customer_web(batch, headers, kintone_url, app_id, user_name, password, csv_encode, batch_number)
    return { success: false, error: "空のバッチ" } if batch.empty?

    batch_csv_path = create_batch_csv_customer_web(batch, headers, batch_number)
    return { success: false, error: "一時ファイルの作成に失敗しました" } unless File.exist?(batch_csv_path)

    command = build_import_command_customer_web(batch_csv_path, kintone_url, app_id, user_name, password, csv_encode)

    project_root = File.expand_path("../..", __dir__)
    success = false
    error_message = nil

    Open3.popen3(command, chdir: project_root) do |_stdin, stdout, stderr, wait_thr|
      stdout_str = stdout.read
      stderr_str = stderr.read
      exit_status = wait_thr.value

      success = exit_status.success?
      error_message = extract_detailed_error_customer_web(stdout_str, stderr_str) unless success
    end

    FileUtils.rm_f(batch_csv_path)
    { success: success, error: error_message }
  end

  def import_single_record_fast_customer_web(record, headers, kintone_url, app_id, user_name, password, csv_encode)
    single_result = { success: false, error: nil }
    temp_csv_path = create_single_record_csv_customer_web(record, headers)
    return single_result.merge(error: "一時ファイルの作成に失敗しました") unless File.exist?(temp_csv_path)

    command = build_import_command_customer_web(temp_csv_path, kintone_url, app_id, user_name, password, csv_encode)
    project_root = File.expand_path("../..", __dir__)

    Open3.popen3(command, chdir: project_root) do |_stdin, stdout, stderr, wait_thr|
      stdout_str = stdout.read
      stderr_str = stderr.read
      exit_status = wait_thr.value
      single_result[:success] = exit_status.success?
      single_result[:error] = extract_detailed_error_customer_web(stdout_str, stderr_str) unless single_result[:success]
    end

    FileUtils.rm_f(temp_csv_path)
    single_result
  end

  def create_batch_csv_customer_web(batch, headers, batch_number)
    temp_dir = Rails.root.join("tmp", "customer_web", "batch_imports")
    FileUtils.mkdir_p(temp_dir)
    timestamp = Time.current.strftime("%Y%m%d%H%M%S")
    filename = "batch_#{batch_number}_#{timestamp}.csv"
    batch_csv_path = File.join(temp_dir, filename)
    begin
      CSV.open(batch_csv_path, "w", encoding: "Shift_JIS") do |csv|
        csv << headers
        batch.each { |record| csv << record }
      end
      @logger.debug "バッチCSVファイル作成成功: #{batch_csv_path} (#{batch.size} レコード)"
      batch_csv_path
    rescue => e
      @logger.error "バッチCSVファイル作成エラー: #{e.message}"
      nil
    end
  end

  def create_single_record_csv_customer_web(record, headers)
    temp_dir = Rails.root.join("tmp", "customer_web", "single_record_imports")
    FileUtils.mkdir_p(temp_dir)
    timestamp = Time.current.strftime("%Y%m%d%H%M%S")
    record_id = extract_record_id_for_mode(record, :customer)
    filename = "single_record_#{record_id}_#{timestamp}.csv"
    temp_csv_path = File.join(temp_dir, filename)
    begin
      CSV.open(temp_csv_path, "w", encoding: "Shift_JIS") do |csv|
        csv << headers
        csv << record
      end
      temp_csv_path
    rescue => e
      @logger.error "単一レコードCSV作成エラー: #{e.message}"
      nil
    end
  end

  def build_import_command_customer_web(csv_path, kintone_url, app_id, user_name, password, csv_encode)
    [
      CLI_PATH,
      "record import --base-url #{Shellwords.escape(kintone_url)}",
      "--app #{Shellwords.escape(app_id)}",
      "--username #{Shellwords.escape(user_name)}",
      "--password #{Shellwords.escape(password)}",
      "--file-path #{Shellwords.escape(csv_path)}",
      "--encoding #{Shellwords.escape(csv_encode)}",
    ].join(" ")
  end

  def extract_detailed_error_customer_web(stdout_str, stderr_str)
    if stdout_str.blank? && stderr_str.blank?
      return "不明なエラー（出力なし）"
    end

    full_output = [stdout_str, stderr_str].compact.join("\n")
    patterns = [
      /error:\s*(.+?)(?=\n|$)/i,
      /エラー[:：]\s*(.+?)(?=\n|$)/i,
      /required field.*?['"](.+?)['"]/i,
      /必須項目.*?['"](.+?)['"]/i,
      /invalid value.*?['"](.+?)['"]/i,
      /不正な値.*?['"](.+?)['"]/i,
      /duplicate.*?value.*?['"](.+?)['"]/i,
      /重複.*?値.*?['"](.+?)['"]/i,
      /not found|見つかりません/i,
      /permission denied|権限がありません/i,
      /authentication failed|認証に失敗/i,
      /timeout|タイムアウト/i,
    ]

    details = patterns.flat_map do |pat|
      full_output.scan(pat).flatten.map(&:strip).reject(&:empty?)
    end.uniq

    if details.any?
      details.uniq.join("; ")
    else
      extract_fallback_error_details(full_output)
    end
  end

  def extract_fallback_error_details(full_output)
    error_lines = full_output.split("\n").select do |line|
      line.match?(/error|エラー|failed|失敗|invalid|不正|required|必須|duplicate|重複/i) &&
        !line.match?(/(usage|使用法|options|オプション|info|debug|warning)/i)
    end

    error_lines.any? ? error_lines.first(3).join("; ") : "詳細不明なエラー: #{full_output[0..200]}..."
  end

  def extract_record_id_for_mode(record, id_mode)
    raw_id = case id_mode
             when :customer
               action_type = record["MinfamiActionType"] || record["minfamiActionType"]
               action_id = record["ActionId"] || record["actionId"]
               action_id_str = action_id.nil? ? "" : action_id.to_s
               "#{action_type}#{action_id_str}"
             when :place_introduce
               record["Name"]
             else
               record["Name"] || record["Id"] || "unknown"
             end

    sanitize_filename(raw_id)
  end

  def sanitize_filename(filename)
    return "unknown" if filename.blank?

    basename = File.basename(filename.to_s)

    if basename.empty? || basename == "." || basename == ".."
      "unknown"
    else
      basename
    end
  end

  def log_final_result_with_ids(result, csv_path, app_id)
    if result[:success]
      @logger.info "インポート完全成功: #{csv_path} -> アプリ#{app_id}"
      @logger.info "   全 #{result[:total]} 行成功"
    else
      if result[:success_count] > 0
        @logger.warn "インポート部分成功: #{csv_path} -> アプリ#{app_id}"
        @logger.warn "   成功: #{result[:success_count]} 行"
        @logger.warn "   失敗: #{result[:error_count]} 行"
        @logger.warn "   失敗ID: #{result[:failed_ids].join(", ")}" if result[:failed_ids].any?
      else
        @logger.error "インポート完全失敗: #{csv_path} -> アプリ#{app_id}"
        @logger.error "   全 #{result[:total]} 行失敗"
      end
    end

    result[:errors].each { |error| @logger.error "   - #{error}" } if result[:errors].any?
  end

  def import_to_kintone(csv_path, app_id)
    unless File.exist?(csv_path)
      @logger.warn "CSVファイルが存在しません: #{csv_path}"
      return
    end

    kintone_url = Settings.kintone_apps.kintone_url
    csv_encode = Settings.kintone_apps.csv_encode
    user_name = Settings.kintone_apps.user_name
    password = Settings.kintone_apps.password

    project_root = File.expand_path("../..", __dir__)
    total_count = count_csv_rows(csv_path)
    @logger.info "インポート開始: #{csv_path} (全 #{total_count} 行)"

    command = [
      CLI_PATH,
      "record import --base-url #{Shellwords.escape(kintone_url)}",
      "--app #{Shellwords.escape(app_id)}",
      "--username #{Shellwords.escape(user_name)}",
      "--password #{Shellwords.escape(password)}",
      "--file-path #{Shellwords.escape(csv_path)}",
      "--encoding #{Shellwords.escape(csv_encode)}",
    ].join(" ")

    import_result = {
      success: false,
      total: total_count,
      success_count: 0,
      error_count: 0,
      errors: [],
    }

    Dir.chdir(project_root) do
      Open3.popen3(command) do |_stdin, stdout, stderr, wait_thr|
        stdout_str = stdout.read
        stderr_str = stderr.read
        exit_status = wait_thr.value

        process_import_result(import_result, stdout_str, stderr_str, exit_status, total_count, csv_path, app_id)
      end
    end

    log_final_result(import_result, csv_path, app_id)
    import_result
  rescue => e
    error_result = {
      success: false,
      total: 0,
      success_count: 0,
      error_count: 0,
      errors: ["システムエラー: #{e.message}"],
    }

    @logger.error "インポート処理中に例外が発生しました: #{e.message}"
    @logger.error e.backtrace.join("\n")

    error_result
  end

  def process_import_result(import_result, stdout_str, stderr_str, exit_status, total_count, csv_path, app_id)
    if exit_status.success?
      success_count = parse_success_count(stdout_str) || total_count

      import_result[:success] = true
      import_result[:success_count] = success_count
      import_result[:error_count] = total_count - success_count

      @logger.info "#{csv_path} をKintoneアプリ #{app_id} に正常にインポートしました"
      @logger.info "インポート結果: 成功 #{success_count}/#{total_count} 行"

      if import_result[:error_count] > 0
        @logger.warn "#{import_result[:error_count]} 行のインポートに失敗しました"
        error_details = parse_error_details(stderr_str)
        import_result[:errors] = error_details
        error_details.each { |error| @logger.error " #{error}" }
      end

      @logger.info "一時CSVファイル #{csv_path} を削除しました"
    else
      import_result[:success] = false
      import_result[:error_count] = total_count
      import_result[:errors] = parse_error_details(stderr_str)

      @logger.error "#{csv_path} のKintoneアプリ #{app_id} へのインポートに失敗しました"
      @logger.error "エラー詳細:"
      import_result[:errors].each { |error| @logger.error "   - #{error}" }
      @logger.error "終了ステータス: #{exit_status}"
      @logger.error "標準出力: #{stdout_str}" if stdout_str && !stdout_str.empty?
      @logger.error "標準エラー: #{stderr_str}" if stderr_str && !stderr_str.empty?
    end
  end

  def initialize_logger
    FileUtils.mkdir_p(LOG_DIR)
    date_str = Time.current.strftime("%Y%m%d")
    log_filename = "sf_insert_customer_web_#{date_str}.log"
    log_path = File.join(LOG_DIR, log_filename)

    file_exists = File.exist?(log_path)

    @logger = Logger.new(log_path, "daily")
    @logger.level = Logger::INFO
    @logger.formatter = proc do |severity, datetime, _progname, msg|
      "[#{datetime.strftime("%Y-%m-%d %H:%M:%S")}] #{severity}: #{msg}\n"
    end

    if file_exists
      @logger.info "既存のログファイルに追記開始: #{log_path}"
      @logger.info "=== 新しいバッチ実行開始 ==="
    else
      @logger.info "新しいログファイル作成完了: #{log_path}"
    end
  end

  def count_csv_rows(csv_path)
    return 0 unless File.exist?(csv_path)
    File.foreach(csv_path).count - 1
  rescue => e
    @logger.warn "CSV行数カウントエラー: #{e.message}"
    0
  end

  def parse_success_count(output)
    if (m = output.match(/SUCCESS:\s*(\d+)\s*records? imported/i))
      m[1].to_i
    elsif (m = output.match(/(\d+)\s*records?\s*processed/i))
      m[1].to_i
    end
  end

  def parse_error_details(error_output)
    return [] if error_output.blank?

    errors = []
    lines = error_output.split("\n").select do |line|
      line.match?(/error|failed|fail|エラー|失敗/i) && !line.match?(/debug|info|warning/i)
    end

    lines.each do |line|
      if (m = line.match(/line\s*(\d+).*?error:\s*(.*)/i))
        errors << "行 #{m[1]}: #{m[2].strip}"
      elsif (m = line.match(/record\s*(\d+).*?error:\s*(.*)/i))
        errors << "レコード #{m[1]}: #{m[2].strip}"
      else
        trimmed = line.strip
        errors << trimmed unless trimmed.empty?
      end
    end
    errors.uniq
  end

  def log_final_result(result, csv_path, app_id)
    if result[:success]
      if result[:error_count] == 0
        @logger.info "インポート完了: #{csv_path} -> アプリ#{app_id} (全 #{result[:total]} 行成功)"
      else
        @logger.warn "インポート部分完了: #{csv_path} -> アプリ#{app_id}"
        @logger.warn "成功: #{result[:success_count]} 行"
        @logger.warn "失敗: #{result[:error_count]} 行"
      end
    else
      @logger.error "インポート失敗: #{csv_path} -> アプリ#{app_id}"
      @logger.error "対象: #{result[:total]} 行全て失敗"
    end
  end

  def find_max_pi_number(place_introduce_records)
    max_number = 0
    return max_number unless place_introduce_records && place_introduce_records["records"]

    place_introduce_records["records"].each do |record|
      name_field = record["Name"]
      next unless name_field && name_field["value"]

      name_value = name_field["value"]
      if name_value =~ /^PI-(\d+)$/
        current_number = ::Regexp.last_match(1).to_i
        max_number = current_number if current_number > max_number
      end
    end

    max_number
  end

  def process_sf_customer_web(sf_customer_web, place_records, customer_records, place_introduce_records, max_pi_number, place_introduce_list, after_process_sf_customer_webs)
    begin
      validate_required_fields(sf_customer_web)

      place_record = find_place_record(sf_customer_web, place_records)
      customer_record = find_customer_record(sf_customer_web, customer_records)

      handle_customer_record_not_found(sf_customer_web, customer_record, after_process_sf_customer_webs)

      check_duplicate_record(sf_customer_web, place_introduce_records)
      validate_place_record(sf_customer_web, place_record)

      create_place_introduce_record_if_needed(sf_customer_web, place_record, customer_record, max_pi_number, place_introduce_list)

    rescue => e
      @logger.error "Kintoneインポート中の例外 (#{sf_customer_web.minfami_action_type}:#{sf_customer_web.action_id}): #{e.message}"
    end
  end

  def validate_required_fields(sf_customer_web)
    @logger.error "ユーザー電話番号が設定されていません。" if sf_customer_web.user_tel.blank?
    @logger.error "アクションIDまたはアクション種別が設定されていません。" if sf_customer_web.action_id.blank? || sf_customer_web.minfami_action_type.blank?
    @logger.error "申込者　姓名が設定されていません。" if sf_customer_web.applicant_name.blank?
  end

  def find_place_record(sf_customer_web, place_records)
    return nil unless sf_customer_web.place.present?

    place_records["records"]&.find do |record|
      record["MwedId"]["value"] == sf_customer_web.place
    end
  end

  def find_customer_record(sf_customer_web, customer_records)
    customer_records["records"]&.find do |record|
      record["UserTel"]["value"] == sf_customer_web.user_tel &&
        record["ApplicantName"]["value"] == sf_customer_web.applicant_name
    end
  end

  def handle_customer_record_not_found(sf_customer_web, customer_record, after_process_sf_customer_webs)
    after_process_sf_customer_webs << sf_customer_web if customer_record.nil?
  end

  def check_duplicate_record(sf_customer_web, place_introduce_records)
    action_id_dec = sf_customer_web.action_id.to_f
    place_introduce_record = place_introduce_records["records"]&.find do |record|
      record["RelationKey"]["value"] == "#{sf_customer_web.minfami_action_type}#{action_id_dec}"
    end

    @logger.error "登録済みの連携データです。" unless place_introduce_record.nil?
  end

  def validate_place_record(sf_customer_web, place_record)
    @logger.error "Kintoneに登録されていない式場IDです。" if sf_customer_web.place.present? && place_record.nil?
  end

  def create_place_introduce_record_if_needed(sf_customer_web, place_record, customer_record, max_pi_number, place_introduce_list)
    return unless sf_customer_web.place.present?

    max_pi_number += 1
    pi_number_str = "PI-#{max_pi_number.to_s.rjust(8, "0")}"

    place_introduce_data = build_place_introduce_data(sf_customer_web, place_record, customer_record, pi_number_str)
    place_introduce_list << place_introduce_data
  end

  def build_place_introduce_data(sf_customer_web, place_record, customer_record, pi_number_str)
    {
      "Name" => { "value" => pi_number_str },
      "RecordType" => { "value" => "Web予約" },
      "Place" => { "value" => place_record["Name"]["value"] },
      "Account" => { "value" => place_record["Account"]["value"] },
      "IsSekou" => { "value" => sf_customer_web.is_sekou },
      "SekouTargetDataFrom" => { "value" => sf_customer_web.sekou_target_data_from },
      "SekouTargetDataTo" => { "value" => sf_customer_web.sekou_target_data_to },
      "IsUCG" => { "value" => sf_customer_web.is_ucg },
      "MinfamiActionType" => { "value" => format_minfami_action_type(sf_customer_web.minfami_action_type) },
      "MinfamiActionDate" => { "value" => parse_date(sf_customer_web.minfami_action_date) },
      "MinfamiVisitDate" => { "value" => parse_date(sf_customer_web.minfami_visit_date) },
      "MinfamiVisitDateDescription" => { "value" => sf_customer_web.minfami_visit_date_description },
      "ActionId" => { "value" => sf_customer_web.action_id.blank? ? nil : sf_customer_web.action_id.to_f },
      "VisitScheduleDatetime" => { "value" => parse_datetime(sf_customer_web.visit_schedule_datetime) },
      "CustomerWeb" => { "value" => customer_record["Name"]["value"] },
      "FairName" => { "value" => sf_customer_web.fair_name },
      "FairVisitPreferredDescription" => { "value" => sf_customer_web.fair_visit_preferred_description },
    }
  end

  def format_minfami_action_type(action_type)
    action_type&.gsub("（", "(")&.gsub("）", ")")&.downcase
  end

  def parse_date(date_string)
    date_string.blank? ? nil : Date.parse(date_string).to_s
  end

  def parse_datetime(datetime_string)
    datetime_string.blank? ? nil : DateTime.parse(datetime_string).iso8601
  end
end

if __FILE__ == $0
  batch = SfInsertCustomerWebBatch.new
  batch.run
end