// DroughtCore/Models/ApiModels.cs
using System;
using System.Collections.Generic;
using System.Text.Json.Serialization; // System.Text.Json 사용 시
// using Newtonsoft.Json; // Newtonsoft.Json 사용 시

namespace DroughtCore.Models
{
    // WAMIS 댐 수문자료 API (mn_hrdata) 응답 구조 예시 (XML 또는 JSON)
    // 실제 응답 구조에 따라 정확하게 모델링 필요.
    // Service.DataCollect.Dam/frmMain.cs 에서는 WAMIS_Controller.GetDamHrData를 호출하여 List<DamHRData>를 받음.
    // OpenAPI.Model 프로젝트의 DamHRData 클래스 등을 참고해야 함.

    // OpenAPI.Model/DamHRData.cs 와 유사하게 정의 (실제 OpenAPI.Model 프로젝트의 내용을 가져와야 함)
    public class WamisDamHourlyApiResponseItem // DamHRData 와 동일하게 간주
    {
        // 예시 필드 (OpenAPI.Model/DamHRData.cs 참고하여 채워야 함)
        [JsonPropertyName("damcd")] // JSON 응답 필드명과 매핑 (System.Text.Json)
        // [JsonProperty("damcd")] // Newtonsoft.Json 사용 시
        public string DamCode { get; set; }

        [JsonPropertyName("obsdh")]
        public string ObservationDateTimeString { get; set; } // "yyyyMMddHH" 형식

        [JsonPropertyName("rsrt")]
        public double? ReservoirStorageRate { get; set; } // 저수율

        [JsonPropertyName("swl")]
        public double? StorageWaterLevel { get; set; } // 저수위

        [JsonPropertyName("inf")]
        public double? InflowTotal { get; set; } // 유입량

        [JsonPropertyName("tototf")]
        public double? TotalOutflow { get; set; } // 총방류량

        // ... 기타 필요한 필드들 ...
    }

    // WAMIS API가 리스트를 포함하는 객체를 반환하는 경우 (예: {"list": [...]})
    public class WamisDamHourlyDataResponse
    {
        [JsonPropertyName("list")]
        public List<WamisDamHourlyApiResponseItem> List { get; set; }
        // 기타 응답 메타데이터 (결과 코드, 메시지 등)가 있다면 추가
        [JsonPropertyName("resultCode")]
        public string ResultCode { get; set; }
        [JsonPropertyName("resultMsg")]
        public string ResultMsg { get; set; }
    }


    // WAMIS 유량 자료 API (flowdtd) 응답 구조 예시
    public class WamisFlowDailyApiResponseItem
    {
        [JsonPropertyName("obscd")]
        public string ObservationSiteCode { get; set; }

        [JsonPropertyName("ymd")]
        public string DateString { get; set; } // "yyyyMMdd"

        [JsonPropertyName("flow")]
        public double? FlowRate { get; set; }
        // ... 기타 필드
    }

    public class WamisFlowDailyDataResponse
    {
        [JsonPropertyName("list")]
        public List<WamisFlowDailyApiResponseItem> List { get; set; }
        [JsonPropertyName("resultCode")]
        public string ResultCode { get; set; }
        [JsonPropertyName("resultMsg")]
        public string ResultMsg { get; set; }
    }

    // --- 나머지 4개 API에 대한 응답 DTO 모델 ---
    // 각 API 제공처의 명세에 따라 JSON 또는 XML 응답 구조를 모델링해야 합니다.
    // 예시: 농업용수공사(EcoWater) API 응답 모델

    public class EcoWaterReservoirLevelItem // 저수지 수위
    {
        [JsonPropertyName("SIGUN_NM")] // 실제 필드명에 맞게 수정
        public string SigunName { get; set; }

        [JsonPropertyName("FACIL_NM")]
        public string FacilityName { get; set; }

        [JsonPropertyName("CHECK_DE")] // YYYYMMDD
        public string CheckDate { get; set; }

        [JsonPropertyName("SAWMNN_RATE")] // 저수율
        public double? StorageRate { get; set; }
        // ...
    }

    public class EcoWaterResponse<T> // 공통 응답 래퍼가 있다면 활용
    {
        [JsonPropertyName("items")] // 또는 "data", "list" 등 실제 필드명
        public List<T> Items { get; set; }

        [JsonPropertyName("pageNo")]
        public int PageNo { get; set; }

        [JsonPropertyName("totalCount")]
        public int TotalCount { get; set; }
        // ...
    }

    // 이런 식으로 각 API의 응답 명세에 맞춰 필요한 DTO들을 정의합니다.
    // 5개 API에 대한 정보가 구체적으로 없으므로, 현재는 WAMIS 위주로 예시를 작성했습니다.
}
