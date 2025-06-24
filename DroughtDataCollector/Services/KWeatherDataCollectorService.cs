// DroughtDataCollector/Services/KWeatherDataCollectorService.cs
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DroughtCore.ApiClients;
using DroughtCore.DataAccess;
using DroughtCore.Logging;
using DroughtCore.Models; // KmaAsosData
using Npgsql;

namespace DroughtDataCollector.Services
{
    public class KWeatherDataCollectorService : BaseDataCollectorService<KmaAsosData, object>
    {
        private readonly KWeatherApiClient _kWeatherClient;

        public KWeatherDataCollectorService(KWeatherApiClient kWeatherClient, DbService dbService, ILogger logger)
            : base(dbService, logger, "KWeatherCollector")
        {
            _kWeatherClient = kWeatherClient;
        }

        protected override async Task<List<string>> GetTargetIdentifiersAsync()
        {
            _logger.Debug("수집 대상 ASOS 지점 코드 목록 조회 시작...", _collectorName);
            // 실제로는 DB의 `tb_kma_asos_thiessen` 테이블에서 DISTINCT code (station_code)를 가져오거나,
            // 또는 `drought_code` 테이블에 기상 관측 지점 정보가 있다면 그곳에서 조회.
            // 여기서는 티센 테이블을 기준으로 한다고 가정.
            // DataTable dt = await _dbService.GetDataTableAsync("SELECT DISTINCT code FROM public.tb_kma_asos_thiessen WHERE code IS NOT NULL ORDER BY code");
            // return dt.AsEnumerable().Select(row => row.Field<string>("code")).ToList();

            // 임시 하드코딩
            await Task.Delay(10);
            var stationCodes = new List<string> { "108", "133" }; // 서울, 대전 예시
            _logger.Debug($"{stationCodes.Count}개의 대상 ASOS 지점 코드 로드됨 (임시).", _collectorName);
            return stationCodes;
        }

        protected override Task<List<KmaAsosData>> FetchDataFromApiAsync(string identifier, DateTime startDate, DateTime endDate)
        {
            // KWeatherApiClient.GetDailyAsosDataAsync는 YYYYMMDD 형식의 문자열을 받음
            return _kWeatherClient.GetDailyAsosDataAsync(identifier, startDate.ToString("yyyyMMdd"), endDate.ToString("yyyyMMdd"));
        }

        protected override string GetLastDataDateQuery(string identifier, out NpgsqlParameter[] parameters)
        {
            parameters = new[] { new NpgsqlParameter("@stn_id", identifier) };
            // 원시 ASOS 데이터 저장 테이블명 및 날짜 컬럼명 확인 필요 (예: public.tb_kma_asos_raw, obs_time_str)
            return "SELECT MAX(obs_time_str) FROM public.tb_kma_asos_raw WHERE station_code = @stn_id";
        }

        protected override bool TryParseLastDate(string dateStr, out DateTime parsedDate)
        {
            // 기상청 API 응답의 날짜 형식이 YYYYMMDD 또는 YYYYMMDDHH 등 다양할 수 있음. API 응답 모델(KmaAsosData)의 ObservationTime 형식에 맞춰야 함.
            // 여기서는 KmaAsosData.ObservationTime이 YYYYMMDDHH 형식이라고 가정. (GetDailyAsosDataAsync는 일별인데, 시간별로 저장할 수도 있으므로)
            // 만약 일별 데이터로 YYYYMMDD만 저장한다면 "yyyyMMdd"로 파싱.
            if (DateTime.TryParseExact(dateStr, "yyyyMMddHH", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out parsedDate))
                return true;
            return DateTime.TryParseExact(dateStr, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out parsedDate);
        }

        protected override DateTime GetNextCollectionDate(DateTime lastDate)
        {
            // KmaAsosData.ObservationTime이 시간 단위라면 AddHours, 일 단위라면 AddDays
            // GetDailyAsosDataAsync를 사용하므로 일 단위로 가정.
            return lastDate.AddDays(1);
        }

        protected override DateTime GetDefaultStartDate() => DateTime.Now.AddMonths(-1); // 예: 최근 1개월치 데이터

        protected override string GetUpsertQuery()
        {
            // 원시 ASOS 데이터 저장 테이블 (예: tb_kma_asos_raw) 스키마에 맞춰 작성
            // PK: (station_code, obs_time_str) - obs_time_str이 YYYYMMDDHH 또는 YYYYMMDD
            // KmaAsosData 모델의 모든 주요 필드를 포함해야 함.
            return @"INSERT INTO public.tb_kma_asos_raw
                     (station_code, obs_time_str, daily_rainfall /*, avg_temp, max_temp, min_temp, avg_wind_speed 등등 */)
                     VALUES (@station_code, @obs_time_str, @daily_rainfall /*, @avg_temp, ... */)
                     ON CONFLICT (station_code, obs_time_str) DO UPDATE SET
                       daily_rainfall = EXCLUDED.daily_rainfall
                       /*, avg_temp = EXCLUDED.avg_temp, ... */ ;";
        }

        protected override NpgsqlParameter[] ConvertToDbParameters(KmaAsosData apiModelData)
        {
            // KmaAsosData 모델의 필드에 맞춰 파라미터 생성
            var parameters = new List<NpgsqlParameter>
            {
                new NpgsqlParameter("@station_code", apiModelData.StationCode),
                new NpgsqlParameter("@obs_time_str", apiModelData.ObservationTime), // YYYYMMDDHH 또는 YYYYMMDD
                new NpgsqlParameter("@daily_rainfall", apiModelData.DailyRainfall.HasValue ? (object)apiModelData.DailyRainfall.Value : DBNull.Value)
                // 다른 필드들 추가...
                // new NpgsqlParameter("@avg_temp", apiModelData.AvgTemp.HasValue ? (object)apiModelData.AvgTemp.Value : DBNull.Value),
            };
            return parameters.ToArray();
        }

        protected override string GetItemIdentifier(KmaAsosData apiModelData)
        {
            return $"Station: {apiModelData.StationCode}, Time: {apiModelData.ObservationTime}";
        }
    }
}
