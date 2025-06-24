// DroughtDataCollector/Program.cs
using System;
using System.Threading.Tasks;
using DroughtCore.Configuration;
using DroughtCore.Logging;
using DroughtCore.ApiClients;
using DroughtCore.DataAccess;
using DroughtCore.Models;
using System.Net.Http;
using System.Collections.Generic;
using Npgsql;
using DroughtDataCollector.Services; // 추가된 서비스 네임스페이스

namespace DroughtDataCollector
{
    class Program
    {
        // static GMLogManagerWrapper logger; // GMLogManager로 대체
        static ConfigManager configManager;

        static async Task Main(string[] args)
        {
            GMLogManager.Configure("log4net.config"); // log4net 설정 파일 로드
            // logger 인스턴스 생성 및 주입 제거

            // 설정 파일에서 로그 레벨을 읽어 GMLogManager에 직접 적용하는 기능은 GMLogManager 내부에 없으므로,
            // log4net.config 파일 자체에서 로그 레벨을 제어해야 함.
            // GMLogManager.SetLogLevel과 같은 메소드가 있다면 사용 가능. (현재 GMLogManager에는 없음)
            GMLogManager.Info("DroughtDataCollector Service 시작", "Program.Main");

            // ConfigManager 생성 시 더 이상 logger 인스턴스 전달 안 함
            configManager = new ConfigManager();

            using (var httpClient = new HttpClient())
            {
                // DbService 생성 시 더 이상 logger 인스턴스 전달 안 함
                var dbService = new DbService(configManager.Settings.ConnectionStrings.PostgreSqlConnection);

                // API 클라이언트 인스턴스화 시 더 이상 logger 인스턴스 전달 안 함
                var wamisApiClient = new WamisApiClient(httpClient, configManager.Settings.ApiKeys.WamisApiKey);
                var ecoWaterApiClient = new EcoWaterApiClient(httpClient, configManager.Settings.ApiKeys.EcoWaterApiKey);
                var kWeatherApiClient = new KWeatherApiClient(httpClient, configManager.Settings.ApiKeys.AnotherApiProviderKey1);
                var soilMoistureApiClient = new SoilMoistureApiClient(httpClient, configManager.Settings.ApiKeys.AnotherApiProviderKey2);
                var riverFlowApiClient = new RiverFlowApiClient(httpClient, configManager.Settings.ApiKeys.AnotherApiProviderKey3);

                // 데이터 수집 서비스 인스턴스화 시 더 이상 logger 인스턴스 전달 안 함
                var wamisDamCollector = new WamisDamDataCollectorService(wamisApiClient, dbService);
                await wamisDamCollector.CollectAndStoreDataAsync();

                var wamisFlowCollector = new WamisFlowDataCollectorService(wamisApiClient, dbService);
                await wamisFlowCollector.CollectAndStoreDataAsync();

                var ecoWaterCollector = new EcoWaterDataCollectorService(ecoWaterApiClient, dbService);
                await ecoWaterCollector.CollectAndStoreDataAsync();

                var kWeatherCollector = new KWeatherDataCollectorService(kWeatherApiClient, dbService);
                await kWeatherCollector.CollectAndStoreDataAsync();

                var soilMoistureCollector = new SoilMoistureDataCollectorService(soilMoistureApiClient, dbService);
                await soilMoistureCollector.CollectAndStoreDataAsync();

                var riverFlowCollector = new RiverFlowDataCollectorService(riverFlowApiClient, dbService);
                await riverFlowCollector.CollectAndStoreDataAsync();

                GMLogManager.Info("모든 데이터 수집 작업 시도 완료.", "Program.Main");
            }

            GMLogManager.Info("DroughtDataCollector Service 종료", "Program.Main");
        }
    }

    public abstract class BaseDataCollectorService<TApiModel, TDbModel>
    {
        protected readonly DbService _dbService;
        // protected readonly ILogger _logger; // GMLogManager로 대체
        protected readonly string _collectorName;

        protected BaseDataCollectorService(DbService dbService, string collectorName) // 로거 인자 제거
        {
            _dbService = dbService;
            // _logger = logger; // GMLogManager로 대체
            _collectorName = collectorName;
        }

        protected abstract Task<List<string>> GetTargetIdentifiersAsync();
        protected abstract Task<List<TApiModel>> FetchDataFromApiAsync(string identifier, DateTime startDate, DateTime endDate);
        protected abstract string GetLastDataDateQuery(string identifier, out NpgsqlParameter[] parameters);
        protected abstract DateTime GetDefaultStartDate();
        protected abstract string GetUpsertQuery();
        protected abstract NpgsqlParameter[] ConvertToDbParameters(TApiModel apiModelData);
        protected abstract string GetItemIdentifier(TApiModel apiModelData);
        protected abstract bool TryParseLastDate(string dateStr, out DateTime parsedDate);
        protected abstract DateTime GetNextCollectionDate(DateTime lastDate);

        public async Task CollectAndStoreDataAsync()
        {
            _logger.Info($"{_collectorName}: 데이터 수집 시작...", _collectorName);
            try
            {
                var targetIdentifiers = await GetTargetIdentifiersAsync();
                if (targetIdentifiers == null || !targetIdentifiers.Any())
                {
                    _logger.Warn($"{_collectorName}: 처리할 대상 식별자가 없습니다.", _collectorName);
                    return;
                }

                foreach (var identifier in targetIdentifiers)
                {
                    DateTime startDate;
                    string lastDateQuery = GetLastDataDateQuery(identifier, out NpgsqlParameter[] lastDateParams);
                    var lastDateStrObj = await _dbService.ExecuteScalarAsync(lastDateQuery, lastDateParams);
                    string lastDateStr = lastDateStrObj?.ToString();

                    if (!string.IsNullOrEmpty(lastDateStr) && TryParseLastDate(lastDateStr, out DateTime lastDateTime))
                    {
                        startDate = GetNextCollectionDate(lastDateTime);
                        _logger.Info($"{_collectorName} - ID {identifier}: 최종 데이터 일자 {lastDateTime:yyyy-MM-dd HH:mm}. 수집 시작: {startDate:yyyy-MM-dd HH:mm}", _collectorName);
                    }
                    else
                    {
                        startDate = GetDefaultStartDate();
                        _logger.Info($"{_collectorName} - ID {identifier}: 최종 데이터 없음. 수집 시작 (기본값): {startDate:yyyy-MM-dd HH:mm}", _collectorName);
                    }

                    DateTime endDate = DateTime.Now;
                    if (startDate >= endDate)
                    {
                        _logger.Info($"{_collectorName} - ID {identifier}: 이미 최신 데이터까지 수집됨. 건너<0xEB><0x9B><0x84>니다.", _collectorName);
                        continue;
                    }

                    List<TApiModel> apiDataList = await FetchDataFromApiAsync(identifier, startDate, endDate);

                    if (apiDataList != null && apiDataList.Any())
                    {
                        _logger.Info($"{_collectorName} - ID {identifier}: {apiDataList.Count} 건 데이터 수집. DB 저장 시작 ({startDate:yyyy-MM-dd HH} ~ {endDate:yyyy-MM-dd HH})...", _collectorName);
                        int newRecords = 0;
                        string upsertQuery = GetUpsertQuery();
                        foreach (var dataItem in apiDataList)
                        {
                            var dbParams = ConvertToDbParameters(dataItem);
                            try
                            {
                                int affected = await _dbService.ExecuteNonQueryAsync(upsertQuery, dbParams);
                                if (affected > 0) newRecords++;
                            }
                            catch (Exception dbEx)
                            {
                                _logger.Error($"{_collectorName} - ID {identifier}, Item {GetItemIdentifier(dataItem)}: DB 저장 실패.", dbEx, _collectorName);
                            }
                        }
                        _logger.Info($"{_collectorName} - ID {identifier}: {newRecords} 건의 신규/업데이트 데이터 DB 저장 완료.", _collectorName);
                    }
                    else
                    {
                        _logger.Info($"{_collectorName} - ID {identifier}: 수집된 신규 데이터 없음.", _collectorName);
                    }
                    await Task.Delay(1000);
                }
            }
            catch (Exception ex)
            {
                _logger.Error($"{_collectorName}: 데이터 수집 중 오류 발생", ex, _collectorName);
            }
            _logger.Info($"{_collectorName}: 데이터 수집 완료.", _collectorName);
        }
    }

    public class WamisDamDataCollectorService : BaseDataCollectorService<DamRawData, object>
    {
        private readonly WamisApiClient _wamisClient;
        public WamisDamDataCollectorService(WamisApiClient wamisClient, DbService dbService, ILogger logger)
            : base(dbService, logger, "WamisDamCollector")
        {
            _wamisClient = wamisClient;
        }

        protected override async Task<List<string>> GetTargetIdentifiersAsync()
        {
            _logger.Debug("수집 대상 댐 코드 목록 조회 시작 (임시 하드코딩)", _collectorName);
            await Task.Delay(10);
            return new List<string> { "1012110", "1001110" };
        }

        protected override Task<List<DamRawData>> FetchDataFromApiAsync(string identifier, DateTime startDate, DateTime endDate)
        {
            return _wamisClient.GetDamHourlyDataAsync(identifier, startDate, endDate);
        }

        protected override string GetLastDataDateQuery(string identifier, out NpgsqlParameter[] parameters)
        {
            parameters = new[] { new NpgsqlParameter("@damcd", identifier) };
            return "SELECT MAX(obsh) FROM public.tb_wamis_mnhrdata WHERE damcd = @damcd";
        }

        protected override bool TryParseLastDate(string dateStr, out DateTime parsedDate)
        {
            return DateTime.TryParseExact(dateStr, "yyyyMMddHH", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out parsedDate);
        }

        protected override DateTime GetNextCollectionDate(DateTime lastDate) => lastDate.AddHours(1);
        protected override DateTime GetDefaultStartDate() => DateTime.Now.AddDays(-30);

        protected override string GetUpsertQuery()
        {
            return @"INSERT INTO public.tb_wamis_mnhrdata
                     (damcd, obsh, swl, rsrt, inf, tototf, observation_datetime)
                     VALUES (@damcd, @obsh, @swl, @rsrt, @inf, @tototf, @observation_datetime)
                     ON CONFLICT (damcd, obsh) DO UPDATE SET
                       swl = EXCLUDED.swl,
                       rsrt = EXCLUDED.rsrt,
                       inf = EXCLUDED.inf,
                       tototf = EXCLUDED.tototf,
                       observation_datetime = EXCLUDED.observation_datetime;";
        }

        protected override NpgsqlParameter[] ConvertToDbParameters(DamRawData apiModelData)
        {
            return new NpgsqlParameter[]
            {
                new NpgsqlParameter("@damcd", apiModelData.DamCode),
                new NpgsqlParameter("@obsh", apiModelData.Obsh),
                new NpgsqlParameter("@swl", apiModelData.Swl.HasValue ? (object)apiModelData.Swl.Value : DBNull.Value),
                new NpgsqlParameter("@rsrt", apiModelData.ReservoirStorageRate.HasValue ? (object)apiModelData.ReservoirStorageRate.Value : DBNull.Value),
                new NpgsqlParameter("@inf", apiModelData.Inf.HasValue ? (object)apiModelData.Inf.Value : DBNull.Value),
                new NpgsqlParameter("@tototf", apiModelData.Tototf.HasValue ? (object)apiModelData.Tototf.Value : DBNull.Value),
                new NpgsqlParameter("@observation_datetime", apiModelData.ObservationDateTime)
            };
        }
        protected override string GetItemIdentifier(DamRawData apiModelData) => $"Dam: {apiModelData.DamCode}, Time: {apiModelData.Obsh}";
    }

    public class WamisFlowDataCollectorService : BaseDataCollectorService<WamisFlowDailyData, object>
    {
        private readonly WamisApiClient _wamisClient;
        public WamisFlowDataCollectorService(WamisApiClient wamisClient, DbService dbService, ILogger logger)
            : base(dbService, logger, "WamisFlowCollector")
        {
            _wamisClient = wamisClient;
        }

        protected override async Task<List<string>> GetTargetIdentifiersAsync()
        {
             _logger.Debug("수집 대상 유량 관측소 목록 조회 시작 (임시 하드코딩)", _collectorName);
            await Task.Delay(10);
            return new List<string> { "1001665", "2001605" };
        }

        protected override Task<List<WamisFlowDailyData>> FetchDataFromApiAsync(string identifier, DateTime startDate, DateTime endDate)
        {
            return _wamisClient.GetFlowDailyDataAsync(identifier, startDate, endDate);
        }

        protected override string GetLastDataDateQuery(string identifier, out NpgsqlParameter[] parameters)
        {
            parameters = new[] { new NpgsqlParameter("@obscd", identifier) };
            return "SELECT MAX(ymd) FROM public.tb_wamis_flowdtdata WHERE obscd = @obscd";
        }

        protected override bool TryParseLastDate(string dateStr, out DateTime parsedDate)
        {
            return DateTime.TryParseExact(dateStr, "yyyyMMdd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out parsedDate);
        }
        protected override DateTime GetNextCollectionDate(DateTime lastDate) => lastDate.AddDays(1);
        protected override DateTime GetDefaultStartDate() => DateTime.Now.AddYears(-1);

        protected override string GetUpsertQuery()
        {
            return @"INSERT INTO public.tb_wamis_flowdtdata
                     (obscd, ymd, flow)
                     VALUES (@obscd, @ymd, @flow)
                     ON CONFLICT (obscd, ymd) DO UPDATE SET
                       flow = EXCLUDED.flow;";
        }

        protected override NpgsqlParameter[] ConvertToDbParameters(WamisFlowDailyData apiModelData)
        {
            return new NpgsqlParameter[]
            {
                new NpgsqlParameter("@obscd", apiModelData.ObservationSiteCode),
                new NpgsqlParameter("@ymd", apiModelData.DateString),
                new NpgsqlParameter("@flow", apiModelData.FlowRate.HasValue ? (object)apiModelData.FlowRate.Value : DBNull.Value)
            };
        }
        protected override string GetItemIdentifier(WamisFlowDailyData apiModelData) => $"Flow: {apiModelData.ObservationSiteCode}, Date: {apiModelData.DateString}";
    }

    // 임시 모델 (실제로는 DroughtCore.Models에 있어야 함)
    public class WamisDamSiteInfo
    {
        public string DamCode { get; set; }
        public string DamName { get; set; }
    }
}
