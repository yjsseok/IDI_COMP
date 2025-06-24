// DroughtCore/Models/DbModels.cs
using System;
using System.ComponentModel.DataAnnotations.Schema; // 예시: [Table], [Column] 어노테이션 사용 시

namespace DroughtCore.Models
{
    // tb_wamis_mnhrdata (WAMIS 댐 수문 원시 데이터)
    // DamRawData.cs 로 이미 일부 정의됨. 필요시 여기에 통합 또는 확장.
    // public class WamisDamHourlyData { ... }

    // tb_kma_asos_dtdata (기상 관측 데이터)
    public class KmaAsosData
    {
        // 실제 테이블 컬럼명과 타입에 맞춰 정의
        [Column("tm")] // 예시: 실제 컬럼명과 속성명이 다를 경우
        public string ObservationTime { get; set; } // YYYYMMDDHH 또는 YYYYMMDD 형태일 수 있음, 파싱 필요

        [Column("stn")]
        public string StationCode { get; set; } // 지점 코드

        [Column("rn_day")]
        public double? DailyRainfall { get; set; } // 일 강수량 (mm)
        // ... 기타 필요한 컬럼들 (온도, 풍속 등)
    }

    // tb_kma_asos_thiessen (티센 망 정보)
    public class ThiessenPolygonData
    {
        [Column("sgg_cd")]
        public string SggCode { get; set; } // 시군구 코드

        [Column("code")]
        public string StationCode { get; set; } // 관측소 코드 (KMA ASOS 지점 코드)

        [Column("ratio")]
        public double ThiessenRatio { get; set; } // 티센 가중치
    }

    // tb_reserviorlevel (저수지 수위 데이터)
    public class ReservoirLevelData
    {
        [Column("fac_code")]
        public string FacilityCode { get; set; } // 시설 코드

        [Column("check_date")]
        public string CheckDate { get; set; } // YYYYMMDD

        [Column("rate")]
        public double? StorageRate { get; set; } // 저수율 (%)

        [Column("water_level")]
        public double? WaterLevel { get; set; } // 수위 (EL.m) - JS_DAMRSRT에서는 사용되지 않음

        [Column("fac_name")]
        public string FacilityName { get; set; } // 시설명 - JS_DAMRSRT에서는 사용되지 않음

        [Column("county")]
        public string County { get; set; } // 지역명 - JS_DAMRSRT에서는 사용되지 않음
    }

    // tb_wamis_flowdtdata (WAMIS 유량 원시 데이터)
    public class WamisFlowDailyData
    {
        [Column("obscd")]
        public string ObservationSiteCode { get; set; } // 관측소 코드

        [Column("ymd")]
        public string DateString { get; set; } // YYYYMMDD

        [Column("flow")]
        public double? FlowRate { get; set; } // 유량 (CMS)
        // ... 기타 필요한 컬럼들
    }

    // drought_code (가뭄 코드 정보 테이블)
    public class DroughtCodeInfo
    {
        [Column("sort")]
        public string Sort { get; set; } // 데이터 종류 (Dam, FR, Ar 등)

        [Column("sgg_cd")]
        public string SggCode { get; set; } // 시군구 코드

        [Column("obs_cd")]
        public string ObservationCode { get; set; } // 관측소/시설 코드 (여러 개일 경우 '_'로 연결)

        [Column("obsnm")]
        public string ObservationName { get; set; } // 관측소/시설 명
        // ... 기타 필요한 컬럼들
    }

    // tb_Actualdrought_DAM (가공된 최종 가뭄 데이터 저장 테이블)
    // 이 테이블은 다양한 종류의 가공된 데이터를 저장하므로, 컬럼 구성이 중요.
    // JS_DAMRSRT의 SaveToDroughtTableWithCopy("tb_Actualdrought_DAM", sgg_cd, lines, strConn) 부분을 보면
    // sgg_cd, yyyy, mm, dd, jd, data 컬럼을 사용함.
    // 'data' 컬럼이 저수율, 유량 등 다양한 값을 저장하므로 double? 타입이 적절.
    public class ActualDroughtDamData
    {
        [Column("sgg_cd")]
        public string SggCode { get; set; }

        [Column("yyyy")]
        public int Year { get; set; }

        [Column("mm")]
        public int Month { get; set; }

        [Column("dd")]
        public int Day { get; set; }

        [Column("jd")]
        public int JulianDay { get; set; }

        [Column("data")] // 이 컬럼에 다양한 종류의 데이터(저수율, 유량 등)가 저장됨
        public double? Value { get; set; }

        // 데이터 타입을 구분할 수 있는 컬럼이 있다면 추가 (예: "DataType" - "DamRsrt", "FlowRate" 등)
        // 현재 JS_DAMRSRT에서는 이 테이블 하나에 모든 종류의 가공 데이터를 넣는 것으로 보임.
        // 만약 데이터 종류별로 테이블을 분리한다면, 각 테이블에 맞는 모델 클래스 필요.
    }
}
