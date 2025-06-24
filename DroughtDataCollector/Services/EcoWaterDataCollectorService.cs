// DroughtDataCollector/Services/EcoWaterDataCollectorService.cs
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DroughtCore.ApiClients;
using DroughtCore.DataAccess;
using DroughtCore.Logging;
using DroughtCore.Models; // EcoWaterReservoirLevelItem, 필요한 경우 DB 모델
using Npgsql; // NpgsqlParameter

namespace DroughtDataCollector.Services
{
    // EcoWater API (예: 저수지 수위 데이터) 수집 서비스
    public class EcoWaterDataCollectorService : BaseDataCollectorService<EcoWaterReservoirLevelItem, object>
    {
        private readonly EcoWaterApiClient _ecoWaterClient;

        public EcoWaterDataCollectorService(EcoWaterApiClient ecoWaterClient, DbService dbService) // 로거 인자 제거
            : base(dbService, "EcoWaterCollector") // 로거 인자 제거
        {
            _ecoWaterClient = ecoWaterClient;
        }

        protected override async Task<List<string>> GetTargetIdentifiersAsync()
        {
            GMLogManager.Debug("수집 대상 저수지 시설 코드 목록 조회 시작...", _collectorName);
            // ... (기존 로직에서 _logger 대신 GMLogManager 사용) ...
            await Task.Delay(10);
            var facilityCodes = new List<string> { "101010", "101020" };
            GMLogManager.Debug($"{facilityCodes.Count}개의 대상 저수지 시설 코드 로드됨 (임시).", _collectorName);
            return facilityCodes;
        }

        protected override Task<List<EcoWaterReservoirLevelItem>> FetchDataFromApiAsync(string identifier, DateTime startDate, DateTime endDate)
        {
            return _ecoWaterClient.GetReservoirLevelAsync(identifier, endDate.ToString("yyyyMMdd"));
        }

        protected override string GetLastDataDateQuery(string identifier, out NpgsqlParameter[] parameters)
        {
            parameters = new[] { new NpgsqlParameter("@fac_code", identifier) };
            return "SELECT MAX(check_date_str) FROM public.tb_ecowater_reservoir_raw WHERE facility_code = @fac_code";
        }

        protected override bool TryParseLastDate(string dateStr, out DateTime parsedDate)
        {
            return DateTime.TryParseExact(dateStr, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out parsedDate);
        }

        protected override DateTime GetNextCollectionDate(DateTime lastDate) => lastDate.AddDays(1);

        protected override DateTime GetDefaultStartDate() => DateTime.Now.AddDays(-7);

        protected override string GetUpsertQuery()
        {
            return @"INSERT INTO public.tb_ecowater_reservoir_raw
                     (facility_code, sigun_name, facility_name, check_date_str, storage_rate, collection_time)
                     VALUES (@facility_code, @sigun_name, @facility_name, @check_date_str, @storage_rate, @collection_time)
                     ON CONFLICT (facility_code, check_date_str) DO UPDATE SET
                       sigun_name = EXCLUDED.sigun_name,
                       facility_name = EXCLUDED.facility_name,
                       storage_rate = EXCLUDED.storage_rate,
                       collection_time = EXCLUDED.collection_time;";
        }

        protected override NpgsqlParameter[] ConvertToDbParameters(EcoWaterReservoirLevelItem apiModelData)
        {
            // API 응답에 fac_code가 있는지 확인 필요. 없다면 identifier를 사용해야 함.
            // EcoWaterReservoirLevelItem에 FacilityCode 속성을 추가하거나, identifier를 이 메소드에 전달해야 함.
            // 임시로 apiModelData.FacilityName을 파싱하거나, identifier를 사용하는 방식으로 가정.
            // 실제로는 apiModelData에 FacilityCode가 있어야 가장 이상적.
            string facilityCode = apiModelData.FacilityCode ?? "UNKNOWN"; // 모델에 FacilityCode가 있다고 가정, 없으면 수정 필요

            return new NpgsqlParameter[]
            {
                new NpgsqlParameter("@facility_code", facilityCode),
                new NpgsqlParameter("@sigun_name", StringUtils.ToStringSafe(apiModelData.SigunName)),
                new NpgsqlParameter("@facility_name", StringUtils.ToStringSafe(apiModelData.FacilityName)),
                new NpgsqlParameter("@check_date_str", StringUtils.ToStringSafe(apiModelData.CheckDate)),
                new NpgsqlParameter("@storage_rate", apiModelData.StorageRate.HasValue ? (object)apiModelData.StorageRate.Value : DBNull.Value),
                new NpgsqlParameter("@collection_time", DateTime.Now)
            };
        }

        protected override string GetItemIdentifier(EcoWaterReservoirLevelItem apiModelData)
        {
            return $"Facility: {apiModelData.FacilityCode ?? "UNKNOWN"}, Date: {apiModelData.CheckDate}";
        }
    }
}
