// DroughtDataCollector/Services/RiverFlowDataCollectorService.cs
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DroughtCore.ApiClients;
using DroughtCore.DataAccess;
using DroughtCore.Logging;
using DroughtCore.Models; // RiverFlowReading
using Npgsql;

namespace DroughtDataCollector.Services
{
    // WAMIS 유량 API 외 다른 하천 유량 API가 있을 경우를 위한 예시 서비스
    public class RiverFlowDataCollectorService : BaseDataCollectorService<RiverFlowReading, object>
    {
        private readonly RiverFlowApiClient _riverFlowClient;

        public RiverFlowDataCollectorService(RiverFlowApiClient riverFlowClient, DbService dbService, ILogger logger)
            : base(dbService, logger, "RiverFlowAltCollector") // Collector 이름 변경
        {
            _riverFlowClient = riverFlowClient;
        }

        protected override async Task<List<string>> GetTargetIdentifiersAsync()
        {
            _logger.Debug("수집 대상 하천 유량 관측소(대안 API) 코드 목록 조회 시작...", _collectorName);
            // 실제로는 DB 또는 설정 파일에서 대상 관측소 코드 목록을 가져와야 함.
            // 예: DataTable dt = await _dbService.GetDataTableAsync("SELECT DISTINCT station_code FROM manage_river_flow_sites_alt");
            // return dt.AsEnumerable().Select(row => row.Field<string>("station_code")).ToList();

            await Task.Delay(10);
            var stationCodes = new List<string> { "RF_STN_A", "RF_STN_B" }; // 예시 관측소 코드
            _logger.Debug($"{stationCodes.Count}개의 대상 관측소 코드 로드됨 (임시).", _collectorName);
            return stationCodes;
        }

        protected override Task<List<RiverFlowReading>> FetchDataFromApiAsync(string identifier, DateTime startDate, DateTime endDate)
        {
            // RiverFlowApiClient.GetRiverFlowAsync는 fromDate, toDate를 받음
            return _riverFlowClient.GetRiverFlowAsync(identifier, startDate, endDate);
        }

        protected override string GetLastDataDateQuery(string identifier, out NpgsqlParameter[] parameters)
        {
            parameters = new[] { new NpgsqlParameter("@station_code", identifier) };
            // 원시 데이터 저장 테이블명 및 날짜 컬럼명 확인 (예: public.tb_river_flow_alt_raw, obs_timestamp)
            return "SELECT MAX(obs_timestamp) FROM public.tb_river_flow_alt_raw WHERE station_code = @station_code";
        }

        protected override bool TryParseLastDate(string dateStr, out DateTime parsedDate)
        {
            // API 응답 및 DB 저장 형식(RiverFlowReading.Timestamp)에 맞춰 파싱
            // ISO 8601 형식을 가정
            return DateTime.TryParse(dateStr, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal, out parsedDate);
        }

        protected override DateTime GetNextCollectionDate(DateTime lastDate) => lastDate.AddMinutes(10); // 예: 10분 단위 데이터라고 가정하고 다음 시간 조회

        protected override DateTime GetDefaultStartDate() => DateTime.Now.AddDays(-1); // 예: 최근 1일치 데이터

        protected override string GetUpsertQuery()
        {
            // 원시 데이터 저장 테이블 (예: tb_river_flow_alt_raw) 스키마에 맞춰 작성
            // PK: (station_code, obs_timestamp)
            // RiverFlowReading 모델의 필드를 포함해야 함.
            return @"INSERT INTO public.tb_river_flow_alt_raw
                     (station_code, obs_timestamp, flow_cms, water_level_m, collection_time)
                     VALUES (@station_code, @obs_timestamp, @flow_cms, @water_level_m, @collection_time)
                     ON CONFLICT (station_code, obs_timestamp) DO UPDATE SET
                       flow_cms = EXCLUDED.flow_cms,
                       water_level_m = EXCLUDED.water_level_m,
                       collection_time = EXCLUDED.collection_time;";
        }

        protected override NpgsqlParameter[] ConvertToDbParameters(RiverFlowReading apiModelData)
        {
            return new NpgsqlParameter[]
            {
                new NpgsqlParameter("@station_code", apiModelData.StationCode),
                new NpgsqlParameter("@obs_timestamp", apiModelData.Timestamp), // DateTime 타입 직접 사용
                new NpgsqlParameter("@flow_cms", apiModelData.FlowCms),
                new NpgsqlParameter("@water_level_m", apiModelData.WaterLevel),
                new NpgsqlParameter("@collection_time", DateTime.Now)
            };
        }

        protected override string GetItemIdentifier(RiverFlowReading apiModelData)
        {
            return $"Station: {apiModelData.StationCode}, Timestamp: {apiModelData.Timestamp:yyyy-MM-dd HH:mm:ss}";
        }
    }
}
