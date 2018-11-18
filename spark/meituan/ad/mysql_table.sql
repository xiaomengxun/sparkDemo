
-- ==================================================================
--              实时累加统计各省份各城市各广告点击次数
-- ==================================================================
-- 实时累加统计各省份各城市各广告点击次数
DROP TABLE IF EXISTS `tb_ad_real_time_state`;
CREATE TABLE `tb_ad_real_time_state` (
  `date` varchar(50) NOT NULL DEFAULT '' COMMENT '日期',
  `province` varchar(50) CHARACTER SET utf8 NOT NULL DEFAULT '' COMMENT '省份',
  `city` varchar(50) CHARACTER SET utf8 NOT NULL DEFAULT '' COMMENT '城市',
  `ad_id` int(11) NOT NULL COMMENT '广告id',
  `click_count` int(11) NOT NULL DEFAULT '0' COMMENT '点击次数',
  PRIMARY KEY (`date`,`province`,`city`,`ad_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- 插入或更新
INSERT INTO tb_ad_real_time_state(`date`, `province`, `city`, `ad_id`, `click_count`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `click_count`=VALUES(`click_count`)

-- ==================================================================
--              计算各个省份Top5的热门广告数据
-- ==================================================================
-- 计算各个省份Top5的热门广告数据（插入数据前需要先依据BatchDate删除后插入）
DROP TABLE IF EXISTS `tb_top5_province_ad_click_count`;
CREATE TABLE `tb_top5_province_ad_click_count` (
  `date` varchar(50) NOT NULL DEFAULT '' COMMENT '日期',
  `province` varchar(50) CHARACTER SET utf8 NOT NULL DEFAULT '' COMMENT '省份',
  `ad_id` int(11) NOT NULL COMMENT '广告id',
  `click_count` int(11) NOT NULL DEFAULT '0' COMMENT '点击次数',
  PRIMARY KEY (`date`,`province`,`ad_id`,`click_count`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- 插入
INSERT INTO tb_top5_province_ad_click_count(`date`, `province`, `ad_id`, `click_count`) VALUES (?, ?, ?, ?)

-- ------------------------------------------------------
-- 计算各个省份Top5的热门广告数据（增加一个Row_Number字段）
DROP TABLE IF EXISTS `tb_top5_province_ad_click_cnt`;
CREATE TABLE `tb_top5_province_ad_click_cnt` (
  `rank_id` int(11) NOT NULL COMMENT '序号，用于表示当前广告在省份中排名',
  `date` varchar(50) NOT NULL DEFAULT '' COMMENT '日期',
  `province` varchar(50) CHARACTER SET utf8 NOT NULL DEFAULT '' COMMENT '省份',
  `ad_id` int(11) NOT NULL COMMENT '广告id',
  `click_count` int(11) NOT NULL DEFAULT '0' COMMENT '点击次数',
  PRIMARY KEY (`rank_id`, `date`,`province`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- 插入
INSERT INTO tb_top5_province_ad_click_cnt(`rank_id`, `date`, `province`, `ad_id`, `click_count`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `ad_id`=VALUES(`ad_id`), `click_count`=VALUES(`click_count`)

-- ==================================================================
--              实时统计最近10分钟的某个广告点击数量
-- ==================================================================
-- 实时统计最近10分钟的某个广告点击数量
DROP TABLE IF EXISTS `tb_ad_click_count_window`;
CREATE TABLE `tb_ad_click_count_window` (
  `start_window_time` varchar(50) NOT NULL DEFAULT '' COMMENT '统计窗口数据开始时间',
  `end_window_time` varchar(50) NOT NULL DEFAULT '' COMMENT '统计窗口数据结束时间',
  `ad_id` int(11) NOT NULL COMMENT '广告id',
  `click_count` int(11) NOT NULL DEFAULT '0' COMMENT '点击次数',
  PRIMARY KEY (`start_window_time`,`ad_id`,`end_window_time`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

INSERT INTO tb_ad_click_count_window(`start_window_time`, `end_window_time`, `ad_id`, `click_count`) VALUES (?, ?, ?, ?)



