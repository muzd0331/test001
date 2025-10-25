# frozen_string_literal: true

require "fileutils"
require "logger"
require "open3"
require "shellwords"
require "csv"
require "parallel"
require_relative "../models/cache/place"

class SfUpsertPlaceBatch < BatchBase
  CLI_PATH = "./cli-kintone"
  UPDATE_KEY = "MwedId"
  LOG_DIR = Rails.root.join("log", "batches")

  def run
    initialize_logger

    begin
      @logger.info "=== SfUpsertPlaceBatch 開始 ==="
      @logger.info "開始時刻: #{Time.current.strftime("%Y-%m-%d %H:%M:%S")}"

      kintone_url = Settings.kintone_apps.kintone_url
      app_id = Settings.kintone_apps.place.app_id
      csv_encode = Settings.kintone_apps.csv_encode
      user_name = Settings.kintone_apps.user_name
      password = Settings.kintone_apps.password

      place_ids = ARGV.dup

      sth = run_api_before_processing(place_ids)
      elems = SfUpsertPlace.get_elems(sth)
      file_url_list = SfUpsertPlace.generate_multiple_csv(elems)

      total_files = file_url_list.length
      @logger.info "合計#{total_files}個のCSVファイルをKintoneにインポートします"

      process_csv_files(file_url_list, kintone_url, app_id, user_name, password, csv_encode)
    ensure
      @logger.info("終了")
      @logger.info "終了時刻: #{Time.current.strftime("%Y-%m-%d %H:%M:%S")}"
    end
  end

  def run_api_before_processing(place_ids)
    place_ids.each do |place_id|
      if place_id && !place_id.empty?
        unless place_id =~ /^[0-9]+$/
          @logger.error "パラメータ'#{place_id}'が無効です。処理を中止しました。指定しないか、'会社ID'に合致する数値を指定してください。"
        end
      end
    end

    place_ids_str = place_ids.compact.join(",")

    sth = SfUpsertPlace.get_sync_place(place_ids_str)
    if sth.empty?
      @logger.error "対応データが'0'件です。処理を中止しました。"
    end
    sth
  end

  def process_csv_files(file_url_list, kintone_url, app_id, user_name, password, csv_encode)
    file_url_list.each_with_index do |file_url, index|
      @logger.info "#{index + 1}/#{file_url_list.length}個目のCSVファイルをKintoneにインポート中..."

      begin
        import_to_kintone_parallel(
          file_url,
          kintone_url,
          app_id,
          user_name,
          password,
          csv_encode,
          100,
          4
        )
      rescue => e
        @logger.error "#{index + 1}個目のファイルのインポートに失敗しました: #{e.message}"
      end
    end
  end

  def import_to_kintone_parallel(csv_path, kintone_url, app_id, user_name, password, csv_encode, batch_size = 100, threads = 4)
    unless File.exist?(csv_path)
      return { success: false, message: "CSVファイルが存在しません: #{csv_path}" }
    end

    records = []
    CSV.foreach(csv_path, headers: true, encoding: "Shift_JIS") do |row|
      records << row
    end

    total_count = records.size
    headers = records[0].headers

    @logger.info "並列インポート開始: #{csv_path} (全 #{total_count} 行, スレッド: #{threads}, バッチサイズ: #{batch_size})"

    batches = records.each_slice(batch_size).to_a

    results = Parallel.map_with_index(batches, in_threads: threads) do |batch, batch_index|
      import_batch_optimized(batch, headers, kintone_url, app_id, user_name, password, csv_encode, batch_index + 1)
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
    log_final_result(import_result, csv_path, app_id)
    import_result
  end

  def import_batch_optimized(batch, headers, kintone_url, app_id, user_name, password, csv_encode, batch_number)
    batch_result = {
      success_count: 0,
      error_count: 0,
      success_ids: [],
      failed_ids: [],
      errors: []
    }

    batch_success = import_batch_direct(batch, headers, kintone_url, app_id, user_name, password, csv_encode, batch_number)

    if batch_success[:success]
      batch_result[:success_count] = batch.size
      batch_result[:success_ids] = batch.map { |r| r["MwedId"] }
      @logger.info "バッチ #{batch_number} 一括インポート成功: #{batch.size} レコード"
    else
      @logger.warn "バッチ #{batch_number} 一括インポート失敗、単一レコード処理に切り替え: #{batch_success[:error]}"
      process_batch_records_individually(batch, headers, kintone_url, app_id, user_name, password, csv_encode, batch_result)
    end

    batch_result
  end

  def process_batch_records_individually(batch, headers, kintone_url, app_id, user_name, password, csv_encode, batch_result)
    batch.each do |record|
      mwed_id = record["MwedId"]

      single_result = import_single_record_fast(
        record, headers, kintone_url, app_id, user_name, password, csv_encode
      )

      if single_result[:success]
        batch_result[:success_count] += 1
        batch_result[:success_ids] << mwed_id
      else
        batch_result[:error_count] += 1
        batch_result[:failed_ids] << mwed_id
        batch_result[:errors] << "レコード (MwedId: #{mwed_id}): #{single_result[:error]}"
      end
    end
  end

  def import_batch_direct(batch, headers, kintone_url, app_id, user_name, password, csv_encode, batch_number)
    return { success: false, error: "空のバッチ" } if batch.empty?

    batch_csv_path = create_batch_csv(batch, headers, batch_number)
    return { success: false, error: "一時ファイルの作成に失敗しました" } unless File.exist?(batch_csv_path)

    command = build_import_command(batch_csv_path, kintone_url, app_id, user_name, password, csv_encode)

    project_root = File.expand_path("../..", __dir__)
    success = false
    error_message = nil

    Open3.popen3(command, chdir: project_root) do |_stdin, stdout, stderr, wait_thr|
      stdout_str = stdout.read
      stderr_str = stderr.read
      exit_status = wait_thr.value

      success = exit_status.success?
      error_message = extract_detailed_error(stdout_str, stderr_str) unless success
    end

    FileUtils.rm_f(batch_csv_path)
    { success: success, error: error_message }
  end

  def create_batch_csv(batch, headers, batch_number)
    temp_dir = Rails.root.join("tmp", "batch_imports")
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

  def import_single_record_fast(record, headers, kintone_url, app_id, user_name, password, csv_encode)
    single_result = { success: false, error: nil }
    temp_csv_path = create_single_record_csv(record, headers)

    return single_result.merge(error: "一時ファイルの作成に失敗しました") unless File.exist?(temp_csv_path)

    command = build_import_command(temp_csv_path, kintone_url, app_id, user_name, password, csv_encode)

    project_root = File.expand_path("../..", __dir__)

    Open3.popen3(command, chdir: project_root) do |_stdin, stdout, stderr, wait_thr|
      stdout_str = stdout.read
      stderr_str = stderr.read
      exit_status = wait_thr.value

      single_result[:success] = exit_status.success?
      single_result[:error] = extract_detailed_error(stdout_str, stderr_str) unless single_result[:success]
    end

    FileUtils.rm_f(temp_csv_path)
    single_result
  end

  def create_single_record_csv(record, headers, record_number = nil)
    timestamp = Time.current.strftime("%Y%m%d%H%M%S")
    record_id = record["MwedId"] || "unknown"
    suffix = record_number ? "_#{record_number}" : ""

    temp_dir = Rails.root.join("tmp", "single_record_imports")
    FileUtils.mkdir_p(temp_dir)

    temp_csv_path = File.join(temp_dir, "single_record_#{record_id}_#{timestamp}#{suffix}.csv")

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

  def build_import_command(csv_path, kintone_url, app_id, user_name, password, csv_encode)
    [
      CLI_PATH,
      "record import --base-url #{Shellwords.escape(kintone_url)}",
      "--app #{Shellwords.escape(app_id)}",
      "--username #{Shellwords.escape(user_name)}",
      "--password #{Shellwords.escape(password)}",
      "--file-path #{Shellwords.escape(csv_path)}",
      "--encoding #{Shellwords.escape(csv_encode)}",
      "--update-key #{UPDATE_KEY}",
    ].join(" ")
  end

  def extract_detailed_error(stdout_str, stderr_str)
    return "不明なエラー（出力なし）" if stdout_str.empty? && stderr_str.empty?

    full_output = [stdout_str, stderr_str].join("\n")

    error_patterns = [
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
      /line\s+(\d+).*?error/i,
      /行\s+(\d+).*?エラー/i,
    ]

    detailed_errors = error_patterns.flat_map do |pattern|
      full_output.scan(pattern).flatten.map(&:strip).reject(&:empty?)
    end.uniq

    if detailed_errors.any?
      detailed_errors.join("; ")
    else
      extract_fallback_error_details(full_output)
    end
  end

  def extract_fallback_error_details(full_output)
    error_lines = full_output.split("\n").select do |line|
      line.match?(/error|エラー|failed|失敗|invalid|不正|required|必須|duplicate|重複/i) &&
        !line.match?(/(usage|使用法|options|オプション|info|debug|warning)/i)
    end

    if error_lines.any?
      error_lines.first(3).join("; ")
    else
      "詳細不明なエラー: #{full_output[0..200]}..."
    end
  end

  def initialize_logger
    FileUtils.mkdir_p(LOG_DIR)
    date_str = Time.current.strftime("%Y%m%d")
    log_filename = "sf_upsert_place_#{date_str}.log"
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

    valid_rows = 0
    begin
      CSV.foreach(csv_path, encoding: "Shift_JIS:UTF-8", headers: true) do |row|
        next if row.nil? || row.to_h.values.all? { |v| v.nil? || v.to_s.strip.empty? }
        valid_rows += 1
      end
    rescue => e
      @logger.warn "CSV解析エラー(精確カウントをフォールバック): #{e.message}"
      return [File.foreach(csv_path).count - 1, 0].max
    end
    valid_rows
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
    return [] if error_output.nil? || error_output.empty?

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

    return unless result[:errors].any?

    @logger.error "エラー詳細:"
    result[:errors].each { |error| @logger.error "   - #{error}" }
  end
end

if __FILE__ == $0
  batch = SfUpsertPlaceBatch.new
  batch.run
end