// DroughtDataCollector/Services/SoilMoistureDataCollectorService.cs
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DroughtCore.ApiClients;
using DroughtCore.DataAccess;
using DroughtCore.Logging;
using DroughtCore.Models; // SoilMoistureData
using Npgsql;

namespace DroughtDataCollector.Services
{
    public class SoilMoistureDataCollectorService : BaseDataCollectorService<SoilMoistureData, object>
    {
        private readonly SoilMoistureApiClient _soilMoistureClient;

        public SoilMoistureDataCollectorService(SoilMoistureApiClient soilMoistureClient, DbService dbService, ILogger logger)
            : base(dbService, logger, "SoilMoistureCollector")
        {
            _soilMoistureClient = soilMoistureClient;
        }

        protected override async Task<List<string>> GetTargetIdentifiersAsync()
        {
            _logger.Debug("수집 대상 토양 수분 관측 지역/지점 코드 목록 조회 시작...", _collectorName);
            // 실제로는 DB 또는 설정 파일에서 대상 지역/지점 코드 목록을 가져와야 함.
            // 예: DataTable dt = await _dbService.GetDataTableAsync("SELECT DISTINCT area_code FROM manage_soil_moisture_sites");
            // return dt.AsEnumerable().Select(row => row.Field<string>("area_code")).ToList();

            // 임시 하드코딩
            await Task.Delay(10);
            var areaCodes = new List<string> { "AREA001", "AREA002" }; // 예시 지역 코드
            _logger.Debug($"{areaCodes.Count}개의 대상 지역 코드 로드됨 (임시).", _collectorName);
            return areaCodes;
        }

        protected override Task<List<SoilMoistureData>> FetchDataFromApiAsync(string identifier, DateTime startDate, DateTime endDate)
        {
            // SoilMoistureApiClient.GetSoilMoistureAsync는 단일 date를 받도록 예시 작성됨.
            // API가 기간 조회를 지원하지 않는다면, startDate부터 endDate까지 하루씩 반복 호출 필요.
            // 여기서는 endDate 하루치만 조회한다고 가정.
            return _soilMoistureClient.GetSoilMoistureAsync(identifier, endDate);
        }

        protected override string GetLastDataDateQuery(string identifier, out NpgsqlParameter[] parameters)
        {
            parameters = new[] { new NpgsqlParameter("@area_code", identifier) };
            // 원시 토양 수분 데이터 저장 테이블명 및 날짜 컬럼명 확인 필요 (예: public.tb_soil_moisture_raw, obs_date_str)
            return "SELECT MAX(obs_date_str) FROM public.tb_soil_moisture_raw WHERE area_code = @area_code";
        }

        protected override bool TryParseLastDate(string dateStr, out DateTime parsedDate)
        {
            // API 응답 및 DB 저장 형식(SoilMoistureData.ObservationDate)에 맞춰 YYYYMMDD로 가정
            return DateTime.TryParseExact(dateStr, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out parsedDate);
        }

        protected override DateTime GetNextCollectionDate(DateTime lastDate) => lastDate.AddDays(1);

        protected override DateTime GetDefaultStartDate() => DateTime.Now.AddDays(-3); // 예: 최근 3일치 데이터

        protected override string GetUpsertQuery()
        {
            // 원시 토양 수분 데이터 저장 테이블 (예: tb_soil_moisture_raw) 스키마에 맞춰 작성
            // PK: (area_code, obs_date_str)
            // SoilMoistureData 모델의 필드를 포함해야 함.
            return @"INSERT INTO public.tb_soil_moisture_raw
                     (area_code, obs_date_str, moisture_volumetric, depth_10cm, depth_20cm, collection_time)
                     VALUES (@area_code, @obs_date_str, @moisture_volumetric, @depth_10cm, @depth_20cm, @collection_time)
                     ON CONFLICT (area_code, obs_date_str) DO UPDATE SET
                       moisture_volumetric = EXCLUDED.moisture_volumetric,
                       depth_10cm = EXCLUDED.depth_10cm,
                       depth_20cm = EXCLUDED.depth_20cm,
                       collection_time = EXCLUDED.collection_time;";
        }

        protected override NpgsqlParameter[] ConvertToDbParameters(SoilMoistureData apiModelData)
        {
            return new NpgsqlParameter[]
            {
                new NpgsqlParameter("@area_code", apiModelData.AreaCode),
                new NpgsqlParameter("@obs_date_str", apiModelData.ObservationDate.ToString("yyyyMMdd")),
                new NpgsqlParameter("@moisture_volumetric", apiModelData.MoistureContentvolumetric),
                new NpgsqlParameter("@depth_10cm", apiModelData.Depth10cm.HasValue ? (object)apiModelData.Depth10cm.Value : DBNull.Value),
                new NpgsqlParameter("@depth_20cm", apiModelData.Depth20cm.HasValue ? (object)apiModelData.Depth20cm.Value : DBNull.Value),
                new NpgsqlParameter("@collection_time", DateTime.Now)
            };
        }

        protected override string GetItemIdentifier(SoilMoistureData apiModelData)
        {
            return $"Area: {apiModelData.AreaCode}, Date: {apiModelData.ObservationDate:yyyyMMdd}";
        }
    }
}
