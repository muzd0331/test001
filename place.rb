class Cache::Place  < BatchBase
  PLACE_INFO = {}
  PLACE_PREF_NUM = {}
  PLACE_AREA_NUM = {}
  AIMITSU_PREF_PLACE_NUM = {}
  AIMITSU_BLOCK_PLACE_NUM = {}

  OPERATING_END_ANNOUNCE_TEXT = {
    0 => '',
    1 => 'この式場は営業を終了しています。',
    2 => 'この式場は婚礼の受付を終了していますが、ホテルは通常営業しています。',
    3 => 'この式場は婚礼の受付を終了していますが、レストランは通常営業しています。',
    4 => 'この式場は婚礼の受付を終了していますが、店舗は通常営業しています。',
    5 => 'この式場はリニューアルオープンしています。新しい式場情報は『新式場名』をご確認ください。'
  };

  OPERATING_MOVE_ANNOUNCE_TEXT = {
    0 => '',
    1 => 'この式場は移転しています。',
    2 => 'この式場は移転しています。新しい式場情報は「移転先式場名」をご確認ください。'
  };

  # インデックス0はnil、インデックス1から47までが都道府県コードに対応
  PREF_BLOCK_ID = [
    nil,
    4,   # 北海道
    5,5,5,5,5,5,  # 東北: 青森,岩手,宮城,秋田,山形,福島
    1,1,1,1,1,1,1,1,  # 関東: 茨城,栃木,群馬,埼玉,千葉,東京,神奈川,山梨
    6,6,6,6,6,  # 中部: 新潟,富山,石川,福井,長野
    3,3,3,3,  # 東海: 岐阜,静岡,愛知,三重
    2,2,2,2,2,2,  # 関西: 滋賀,京都,大阪,兵庫,奈良,和歌山
    7,7,7,7,7,7,7,7,7,  # 中国・四国: 鳥取,島根,岡山,広島,山口,徳島,香川,愛媛,高知
    8,8,8,8,8,8,8,8  # 九州・沖縄: 福岡,佐賀,長崎,熊本,大分,宮崎,鹿児島,沖縄
  ].freeze
  
  # 披露宴スタイル
  FORM_NAME = {
    1   => '専門式場',
    2   => 'ホテル',
    3   => 'レストラン',
    4   => 'ゲストハウス',
    5   => '国内リゾート',
    6   => '神社・仏閣',
    7   => 'チャペル・教会',
    99  => 'その他'
  }.freeze

  # 挙式スタイル
  STYLE_NAME = {
    'church' => '教会',
    'god' => '神前',
    'public' => '人前',
    'buddha' => '仏前',
    'etc' => 'その他'
  }.freeze

  # 挙式スタイル
  STYLE_NAME_SHIKI = {
    'church' => '教会式',
    'god' => '神前式',
    'public' => '人前式',
    'buddha' => '仏前式',
    'etc' => 'その他'
  }.freeze

  RANKING_AREA_TYPE_1 = '8,9,10,11,12,13,14,15'       # 首都圏エリア番号
  RANKING_AREA_TYPE_2 = '25,26,27,28,29,30'           # 関西エリア番号
  RANKING_AREA_TYPE_3 = '21,22,23,24'                 # 東海エリア番号
  RANKING_AREA_TYPE_4 = '1'                           # 北海道エリア番号
  RANKING_AREA_TYPE_5 = '2,3,4,5,6,7'                 # 東北エリア番号
  RANKING_AREA_TYPE_6 = '16,17,18,19,20'              # 北信越エリア番号
  RANKING_AREA_TYPE_7 = '31,32,33,34,35,36,37,38,39'  # 中部・四国エリア番号
  RANKING_AREA_TYPE_8 = '40,41,42,43,44,45,46,47'     # 九州・沖縄エリア番号

  BLOCK_LIST = [
    nil,
    { block_id: 1, seq: 4, mapping: RANKING_AREA_TYPE_1, block_name: '首都圏',   block_name_r: 'shutoken'     },  # 首都圏エリア
    { block_id: 2, seq: 6, mapping: RANKING_AREA_TYPE_2, block_name: '関西',     block_name_r: 'kansai'       },  # 関西エリア
    { block_id: 3, seq: 5, mapping: RANKING_AREA_TYPE_3, block_name: '東海',     block_name_r: 'tokai'        },  # 東海エリア
    { block_id: 4, seq: 1, mapping: RANKING_AREA_TYPE_4, block_name: '北海道',   block_name_r: 'hokkaido'     },  # 北海道エリア
    { block_id: 5, seq: 2, mapping: RANKING_AREA_TYPE_5, block_name: '東北',     block_name_r: 'tohoku'       },  # 東北エリア
    { block_id: 6, seq: 3, mapping: RANKING_AREA_TYPE_6, block_name: '北信越',   block_name_r: 'hokushinetsu' },  # 北信越エリア
    { block_id: 7, seq: 7, mapping: RANKING_AREA_TYPE_7, block_name: '中・四国', block_name_r: 'chushikoku'   },  # 中国・四国エリア
    { block_id: 8, seq: 8, mapping: RANKING_AREA_TYPE_8, block_name: '九州',     block_name_r: 'kyushu'       }   # 九州・沖縄エリア
  ].freeze

  # キャッシュ更新
  def self.update_cache
    PLACE_INFO.clear
    PLACE_PREF_NUM.clear
    PLACE_AREA_NUM.clear
    AIMITSU_PREF_PLACE_NUM.clear
    AIMITSU_BLOCK_PLACE_NUM.clear

    chkword1 = "営業終了"
    chkword2 = "休館"
    places = ::Place
      .joins('LEFT JOIN m_pref ON m_place.pref_id = m_pref.pref_id')
      .where(del_flg: 0)
      .select("
        m_place.place_id,           m_place.place_name,         m_place.place_name_old, m_place.name_index,
        m_place.reading_name,
        m_place.zip_code,           m_place.tel,                m_place.pref_id,        m_place.area_id,
        m_place.address1,           m_place.address2,           m_place.address3,
        m_place.neigh_station1,     m_place.neigh_station2,     m_place.neigh_station3,
        m_place.hp_url,             m_place.form_id,
        m_place.church,             m_place.god,                m_place.public,         m_place.buddha,         m_place.musubi,       m_place.etc,
        m_place.foursis,            m_place.foursis_perm,       m_place.ekitan_pc_url,  m_place.ekitan_mb_url,
        m_place.misc_url1,          m_place.misc_url2,          m_place.misc_url3,      m_place.misc_url4,      m_place.misc_url5,
        m_place.album_id,           m_pref.pref_name,
        m_place.rev_cnt,            m_place.point_total,        m_place.x,              m_place.y,
        m_place.sub_pref_id,
        m_place.sub_address1,       m_place.sub_address2,       m_place.sub_address3,
        m_place.sub_neigh_station1, m_place.sub_neigh_station2, m_place.sub_neigh_station3,
        m_place.sub_x,              m_place.sub_y,
        m_place.sub_remark,
        m_place.att_point, m_place.wedding_style,
        m_place.dt_area_id_s, m_place.dt_area_id_a,
        m_place.sp_hp_url, m_place.brideal_flg,
        m_place.count_review_type_activity,
        m_place.count_review_type_visited,
        m_place.count_review_type_invited,
        m_place.count_review_type_couple,
        ROUND(m_place.point_total, 2) AS all_eval_total,
        m_place.point1 AS eval_point1,
        m_place.point2 AS eval_point2,
        m_place.point3 AS eval_point3,
        m_place.point4 AS eval_point4,
        m_place.point5 AS eval_point5,
        m_place.point6 AS eval_point6,
        m_place.point7 AS eval_point7,
        m_place.operating_state,
        m_place.operating_now_announce_text,
        m_place.operating_now_announce_limit,
        m_place.operating_end_announce_text_id,
        m_place.operating_end_announce_limit,
        m_place.operating_move_announce_text_id,
        m_place.operating_move_announce_limit,
        m_place.operating_move_announce_place_id,
        m_place.display_place_name_yomigana,
        m_place.display_brand_name
      ")
      .to_a

    places.each do |place|
      r_hash = place.attributes.symbolize_keys

      # 星評価計算
      r_hash[:star_class], r_hash[:star_kind] = review_star_image_by_review_point(r_hash[:point_total])
      (1..7).each do |i|
        point = r_hash[:"eval_point#{i}"]
        r_hash[:"star_class#{i}"], r_hash[:"star_kind#{i}"] = review_star_image_by_review_point(point)
      end

      # 名称処理
      r_hash[:place_name_new] = r_hash[:place_name]
      if r_hash[:place_name_old].present?
        r_hash[:place_name] += " （旧名：#{r_hash[:place_name_old]}）"
      end

      # JavaScriptエスケープ
      r_hash[:place_name_js] = r_hash[:place_name].gsub("'", "'\\\\'")
      r_hash[:place_name_new_js] = r_hash[:place_name_new].gsub("'", "'\\\\'")

      # 住所結合
      r_hash[:address] = "#{r_hash[:pref_name]}#{r_hash[:address1]}#{r_hash[:address2]} #{r_hash[:address3]}"

      # 披露宴スタイル
      r_hash[:form_name] = FORM_NAME[r_hash[:form_id]]

      # 挙式スタイル
      style_list = []
      style_list_shiki = []
      %w[church god public buddha etc].each do |style|
        next unless r_hash[style.to_sym] != 0
        style_list << STYLE_NAME[style]
        style_list_shiki << STYLE_NAME_SHIKI[style]
      end
      r_hash[:style_name] = style_list.join(',').gsub(",","\n")
      r_hash[:style_name_shiki] = style_list_shiki.join(',').gsub(",","\n")

      # 最寄駅
      neigh_st_list = []
      (1..3).each do |i|
        station = r_hash[:"neigh_station#{i}"]
        neigh_st_list << station if station.present?
      end
      r_hash[:neigh_station] = neigh_st_list.join(',')

      # 受付最寄駅
      sub_neigh_st_list = []
      (1..3).each do |i|
        station = r_hash[:"sub_neigh_station#{i}"]
        sub_neigh_st_list << station if station.present?
      end
      r_hash[:sub_neigh_station] = sub_neigh_st_list.join(',')

      # モバイル用住所
      r_hash[:mb_address] = "#{r_hash[:pref_name]}#{r_hash[:address1]}"

      # ブロックID
      r_hash[:area] = r_hash[:block_id] = PREF_BLOCK_ID[r_hash[:pref_id]]

      # 都道府県別式場数カウント
      if r_hash[:pref_id] && r_hash[:place_id] > 10000
        PLACE_PREF_NUM[r_hash[:pref_id]] ||= 0
        PLACE_PREF_NUM[r_hash[:pref_id]] += 1
      end

      # 休業チェック
      unless r_hash[:place_name].include?(chkword1) || r_hash[:place_name].include?(chkword2)
        r_hash[:end_flg] = 1
      end

      # 営業状態メッセージ
      r_hash[:operating_end_announce_text] = OPERATING_END_ANNOUNCE_TEXT[r_hash[:operating_end_announce_text_id]]
      r_hash[:operating_move_announce_text] = OPERATING_MOVE_ANNOUNCE_TEXT[r_hash[:operating_move_announce_text_id]]

      # 移転チェック
      if r_hash[:operating_state] == 2 && r_hash[:operating_move_announce_text_id] == 2
        r_hash[:operating_move_announce_text_place_flg] = 1
      end

      # SEO用変数
      r_hash[:place_name_plus_yomigana] = r_hash[:place_name]
      if r_hash[:display_place_name_yomigana].present?
        r_hash[:place_name_plus_yomigana] += "（#{r_hash[:display_place_name_yomigana]}）"
      end

      r_hash[:place_name_plus_brand_name] = r_hash[:place_name]
      if r_hash[:display_brand_name].present?
        r_hash[:place_name_plus_brand_name] += "（#{r_hash[:display_brand_name]}）"
      end

      r_hash[:place_meta_title] = r_hash[:place_name_plus_brand_name]

      PLACE_INFO[r_hash[:place_id]] = r_hash
    end

    # エリア別式場数集計
    BLOCK_LIST.each_with_index do |block, bid|
      next if bid == 0
      blocks = block[:mapping].split(',')
      blocks.each do |block_id|
        PLACE_AREA_NUM[bid] ||= 0
        PLACE_AREA_NUM[bid] += PLACE_PREF_NUM[block_id.to_i] || 0
        PLACE_AREA_NUM[0] ||= 0
        PLACE_AREA_NUM[0] += PLACE_PREF_NUM[block_id.to_i] || 0
      end
    end

    # 式場広告情報取得
    ad_places = ::PlaceAd
      .joins('LEFT JOIN m_place ON ad_place.place_id = m_place.place_id')
      .where('ad_place.ad_st > 0')
      .where(ad_place: { del_flg: 0 })
      .where(m_place: { del_flg: 0 })
      .select("
        ad_place.place_id,
        ad_place.ad_st, ad_place.catch_txt, ad_place.description,
        ad_place.open_info, ad_place.holiday_info,
        ad_place.pickup_spec1, ad_place.pickup_spec2, ad_place.pickup_spec3,
        ad_place.pickup_spec4, ad_place.pickup_spec5,
        ad_place.phone_no_orig, ad_place.phone_no_p, ad_place.phone_no_m, ad_place.phone_no_s,
        ad_place.ppc_st, ad_place.ppc_memo,
        ad_place.course, ad_place.official_st,
        ad_place.phone_no_a,
        ad_place.sub_course, ad_place.sub_open_info, ad_place.sub_holiday_info,
        ad_place.pickup_opt_st,
        ad_place.pickup_opt_stpb_date, ad_place.pickup_opt_edpb_date,
        ad_place.pickup_opt_img,
        ad_place.pickup_opt_catch, ad_place.pickup_opt_catch_smt,
        ad_place.pickup_opt_url, ad_place.pickup_opt_newwin,
        ad_place.phone_no_s, ad_place.phone_no_j,
        ad_place.price_min, ad_place.price_max,
        ad_place.num_min, ad_place.num_max, ad_place.carry_st,
        ad_place.aimitsu_st, m_place.pref_id,
        ad_place.phone_no_t,
        ad_place.official_movie_st,
        ad_place.official_movie_start_datetime,
        ad_place.official_movie_end_datetime,
        ad_place.user_contact,
        ad_place.charge_construction,
        UNIX_TIMESTAMP(ad_place.charge_construction_start_at) AS charge_construction_start_at,
        UNIX_TIMESTAMP(ad_place.charge_construction_end_at) AS charge_construction_end_at,
        ad_place.phone_no_outside,
        ad_place.phone_no_u,
        ad_place.phone_no_q,
        ad_place.sending_from_desk,
        UNIX_TIMESTAMP(ad_place.sending_from_desk_start_at) AS sending_from_desk_start_at,
        UNIX_TIMESTAMP(ad_place.sending_from_desk_end_at) AS sending_from_desk_end_at
      ")
      .to_a
      

    ad_places.each do |ad_place|
      place_id = ad_place.place_id
      PLACE_INFO[place_id] ||= {}
      
      PLACE_INFO[place_id][:ad_st] = ad_place.ad_st
      PLACE_INFO[place_id][:catch_txt] = ad_place.catch_txt
      PLACE_INFO[place_id][:description] = ad_place.description
      PLACE_INFO[place_id][:open_info] = ad_place.open_info
      PLACE_INFO[place_id][:holiday_info] = ad_place.holiday_info
      PLACE_INFO[place_id][:pickup_spec1] = ad_place.pickup_spec1
      PLACE_INFO[place_id][:pickup_spec2] = ad_place.pickup_spec2
      PLACE_INFO[place_id][:pickup_spec3] = ad_place.pickup_spec3
      PLACE_INFO[place_id][:pickup_spec4] = ad_place.pickup_spec4
      PLACE_INFO[place_id][:pickup_spec5] = ad_place.pickup_spec5
      PLACE_INFO[place_id][:phone_no_orig] = ad_place.phone_no_orig
      PLACE_INFO[place_id][:phone_no_p] = ad_place.phone_no_p
      PLACE_INFO[place_id][:phone_no_m] = ad_place.phone_no_m
      PLACE_INFO[place_id][:phone_no_s] = ad_place.phone_no_s
      PLACE_INFO[place_id][:ppc_st] = ad_place.ppc_st
      PLACE_INFO[place_id][:ppc_memo] = ad_place.ppc_memo
      PLACE_INFO[place_id][:course] = ad_place.course
      PLACE_INFO[place_id][:official_st] = ad_place.official_st
      PLACE_INFO[place_id][:phone_no_a] = ad_place.phone_no_a
      PLACE_INFO[place_id][:sub_course] = ad_place.sub_course
      PLACE_INFO[place_id][:sub_open_info] = ad_place.sub_open_info
      PLACE_INFO[place_id][:sub_holiday_info] = ad_place.sub_holiday_info
      PLACE_INFO[place_id][:pickup_opt_st] = ad_place.pickup_opt_st
      PLACE_INFO[place_id][:pickup_opt_stpb_date] = ad_place.pickup_opt_stpb_date
      PLACE_INFO[place_id][:pickup_opt_edpb_date] = ad_place.pickup_opt_edpb_date
      PLACE_INFO[place_id][:pickup_opt_img] = ad_place.pickup_opt_img
      PLACE_INFO[place_id][:pickup_opt_catch] = ad_place.pickup_opt_catch
      PLACE_INFO[place_id][:pickup_opt_catch_smt] = ad_place.pickup_opt_catch_smt
      PLACE_INFO[place_id][:pickup_opt_url] = ad_place.pickup_opt_url
      PLACE_INFO[place_id][:pickup_opt_newwin] = ad_place.pickup_opt_newwin
      PLACE_INFO[place_id][:phone_no_s] = ad_place.phone_no_s
      PLACE_INFO[place_id][:phone_no_j] = ad_place.phone_no_j
      PLACE_INFO[place_id][:price_min] = ad_place.price_min
      PLACE_INFO[place_id][:price_max] = ad_place.price_max
      PLACE_INFO[place_id][:num_min] = ad_place.num_min
      PLACE_INFO[place_id][:num_max] = ad_place.num_max
      PLACE_INFO[place_id][:carry_st] = ad_place.carry_st
      PLACE_INFO[place_id][:aimitsu_st] = ad_place.aimitsu_st
      PLACE_INFO[place_id][:phone_no_t] = ad_place.phone_no_t
      PLACE_INFO[place_id][:official_movie_st] = ad_place.official_movie_st
      PLACE_INFO[place_id][:official_movie_start_datetime] = ad_place.official_movie_start_datetime
      PLACE_INFO[place_id][:official_movie_end_datetime] = ad_place.official_movie_end_datetime
      PLACE_INFO[place_id][:user_contact] = ad_place.user_contact
      PLACE_INFO[place_id][:charge_construction] = ad_place.charge_construction
      PLACE_INFO[place_id][:charge_construction_start_at] = ad_place.charge_construction_start_at
      PLACE_INFO[place_id][:charge_construction_end_at] = ad_place.charge_construction_end_at
      PLACE_INFO[place_id][:phone_no_outside] = ad_place.phone_no_outside
      PLACE_INFO[place_id][:phone_no_u] = ad_place.phone_no_u
      PLACE_INFO[place_id][:phone_no_q] = ad_place.phone_no_q
      PLACE_INFO[place_id][:sending_from_desk] = ad_place.sending_from_desk
      PLACE_INFO[place_id][:sending_from_desk_start_at] = ad_place.sending_from_desk_start_at
      PLACE_INFO[place_id][:sending_from_desk_end_at] = ad_place.sending_from_desk_end_at

      # あいみつカウント
      if ad_place.aimitsu_st == 1 && PLACE_INFO[place_id][:ad_st] == 1 && place_id >= 10000
        pref_id = ad_place.pref_id
        AIMITSU_PREF_PLACE_NUM[pref_id] ||= 0
        AIMITSU_PREF_PLACE_NUM[pref_id] += 1
        
        block_id = PREF_BLOCK_ID[pref_id]
        AIMITSU_BLOCK_PLACE_NUM[block_id] ||= 0
        AIMITSU_BLOCK_PLACE_NUM[block_id] += 1
      end
    end

    # クライアント情報取得
    clients = ::Client
      .joins('LEFT JOIN m_place ON b_client.place_id = m_place.place_id')
      .where(m_place: { del_flg: 0 })
      .where(b_client: { del_flg: 0 })
      .select(
        'b_client.place_id', 'b_client.client_id',
        'b_client.avail_inq', 'b_client.avail_rsv', 'b_client.avail_cata', 'b_client.avail_url', 'b_client.avail_fair',
        'b_client.plan_type',
        'b_client.plan_basic_op1', 'b_client.plan_basic_op2', 'b_client.plan_basic_op3',
        'b_client.plan_basic_op4', 'b_client.plan_basic_op5', 'b_client.plan_basic_op6',
        'b_client.plan_basic_op7', 'b_client.plan_basic_op8', 'b_client.plan_basic_op9',
        'b_client.plan_basic_op10',
        'b_client.new_plan_type',
        'b_client.mieruka_st', 'b_client.own_site_st',
        'b_client.ohidori_pb_num', 'b_client.photo_pb_num', 'b_client.plan_pb_num',
        'b_client.plan_reg_num',
        'b_client.visit_privilege_regist_number',
        'b_client.visit_privilege_published_number',
        'b_client.contract_privilege_regist_number',
        'b_client.contract_privilege_published_number'
      )
      .to_a

    clients.each do |client|
      place_id = client.place_id
      PLACE_INFO[place_id] ||= {}
      
      PLACE_INFO[place_id][:client_id] = client.client_id
      PLACE_INFO[place_id][:avail_inq] = client.avail_inq
      PLACE_INFO[place_id][:avail_rsv] = client.avail_rsv
      PLACE_INFO[place_id][:avail_cata] = client.avail_cata
      PLACE_INFO[place_id][:avail_url] = client.avail_url
      PLACE_INFO[place_id][:avail_fair] = client.avail_fair
      PLACE_INFO[place_id][:plan_type] = client.plan_type == 0 ? 1 : client.plan_type
      PLACE_INFO[place_id][:plan_basic_op1] = client.plan_basic_op1
      PLACE_INFO[place_id][:plan_basic_op2] = client.plan_basic_op2
      PLACE_INFO[place_id][:plan_basic_op3] = client.plan_basic_op3
      PLACE_INFO[place_id][:plan_basic_op4] = client.plan_basic_op4
      PLACE_INFO[place_id][:plan_basic_op5] = client.plan_basic_op5
      PLACE_INFO[place_id][:plan_basic_op6] = client.plan_basic_op6
      PLACE_INFO[place_id][:plan_basic_op7] = client.plan_basic_op7
      PLACE_INFO[place_id][:plan_basic_op8] = client.plan_basic_op8
      PLACE_INFO[place_id][:plan_basic_op9] = client.plan_basic_op9
      PLACE_INFO[place_id][:plan_basic_op10] = client.plan_basic_op10
      PLACE_INFO[place_id][:new_plan_type] = client.new_plan_type
      PLACE_INFO[place_id][:mieruka_st] = client.mieruka_st
      PLACE_INFO[place_id][:own_site_st] = client.own_site_st
      PLACE_INFO[place_id][:ohidori_pb_num] = client.ohidori_pb_num
      PLACE_INFO[place_id][:photo_pb_num] = client.photo_pb_num
      PLACE_INFO[place_id][:plan_pb_num] = client.plan_pb_num
      PLACE_INFO[place_id][:plan_reg_num] = client.plan_reg_num
      PLACE_INFO[place_id][:visit_privilege_regist_number] = client.visit_privilege_regist_number
      PLACE_INFO[place_id][:visit_privilege_published_number] = client.visit_privilege_published_number
      PLACE_INFO[place_id][:contract_privilege_regist_number] = client.contract_privilege_regist_number
      PLACE_INFO[place_id][:contract_privilege_published_number] = client.contract_privilege_published_number
    end
  end

  def self.review_star_image_by_review_point(point)
    # nil値の処理、デフォルトは0点
    point ||= 0.0

    # クラス名の指定
    star_class = "star"

    # ★種別の設定
    star_kinds = [
      { threshold: 4.75, image: "50" },
      { threshold: 4.25, image: "45" },
      { threshold: 3.75, image: "40" },
      { threshold: 3.25, image: "35" },
      { threshold: 2.75, image: "30" },
      { threshold: 2.25, image: "25" },
      { threshold: 1.75, image: "20" },
      { threshold: 1.25, image: "15" },
      { threshold: 1,    image: "10" },
      { threshold: 0,    image: "00" }
    ]

    star_kind = nil
    star_kinds.each do |star|
      if point >= star[:threshold]
        star_kind = star[:image]
        break
      end
    end
    [star_class, star_kind]
  end
end