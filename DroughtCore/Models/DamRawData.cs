// DroughtCore/Models/DamRawData.cs
using System;

namespace DroughtCore.Models
{
    public class DamRawData
    {
        public string DamCode { get; set; } // 댐 코드 (tb_wamis_mnhrdata의 damcd)
        public DateTime ObservationDateTime { get; set; } // 관측일시 (obsdh)
        public double? ReservoirStorageRate { get; set; } // 저수율 (rsrt)
        // ... WAMIS 댐 수문 데이터 API 응답에 따른 추가 필드들 ...

        // 예시: tb_wamis_mnhrdata 테이블과 유사하게 구성
        public string Obsh { get; set; } // 원본 관측일시 문자열 (YYYYMMDDHH)
        public double? Swl { get; set; } // 저수위 (단위: EL.m)
        public double? Inf { get; set; } // 유입량 (단위: CMS)
        public double? Tototf { get; set; } // 총방류량 (단위: CMS)
        public double? Ecpc { get; set; } // 발전량 (단위: 백만kWh) - 사용하지 않을 수 있음
        // ... 기타 필요한 필드들
    }

    public class RainfallData // 예시: 면적 강우 데이터 모델
    {
        public string SggCode { get; set; }
        public DateTime Date { get; set; }
        public int JulianDay { get; set; }
        public double AreaRainfallAmount { get; set; }
        // ... 각 관측소별 강우량 데이터 (동적으로 추가되거나 별도 리스트/사전으로 관리)
    }

    // ... 기타 API 응답 및 DB 테이블에 매핑될 모델들 ...
    // 예: ProcessedDamData, FlowRateData, AgriculturalWaterData, DroughtIndex 등
}
