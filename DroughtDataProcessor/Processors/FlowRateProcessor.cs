// DroughtDataProcessor/Processors/FlowRateProcessor.cs
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DroughtCore.DataAccess;
using DroughtCore.Logging;
using DroughtCore.Models;    // DroughtCodeInfo, WamisFlowDailyData
using DroughtCore.Utils;    // DateTimeUtils
using Npgsql;
using NpgsqlTypes;

namespace DroughtDataProcessor.Processors
{
    public class FlowRateProcessor
    {
        private readonly DbService _dbService;
        private readonly ILogger _logger;
        private readonly string _outputCsvDirectory;
        private const int StartYear = 1991;

        public FlowRateProcessor(DbService dbService, ILogger logger, string outputCsvDirectory)
        {
            _dbService = dbService;
            _logger = logger;
            _outputCsvDirectory = outputCsvDirectory;
            if (!Directory.Exists(_outputCsvDirectory))
            {
                Directory.CreateDirectory(_outputCsvDirectory);
            }
        }

        public async Task ProcessDataAsync()
        {
            _logger.Info("유량(FR) 데이터 처리 시작...", "FlowRateProcessor");
            try
            {
                // 1. 모든 유량 데이터의 마스터 종료일 찾기 (tb_wamis_flowdtdata)
                string maxDateQuery = "SELECT MAX(ymd) FROM public.tb_wamis_flowdtdata";
                var maxDateResult = await _dbService.ExecuteScalarAsync(maxDateQuery);
                if (maxDateResult == null || maxDateResult == DBNull.Value || string.IsNullOrWhiteSpace(maxDateResult.ToString()))
                {
                    _logger.Warn("유량 데이터의 최종 기준일을 찾을 수 없어 처리를 중단합니다.", "FlowRateProcessor");
                    return;
                }
                DateTime masterEndDate = DateTime.ParseExact(maxDateResult.ToString(), "yyyyMMdd", CultureInfo.InvariantCulture);
                _logger.Info($"모든 유량 파일의 최종 기준일: {masterEndDate:yyyy-MM-dd}", "FlowRateProcessor");

                // 2. drought_code 테이블에서 sort = 'FR' 인 sgg_cd 및 obs_cd 목록 가져오기
                string droughtCodeQuery = "SELECT sgg_cd, obs_cd FROM public.drought_code WHERE sort = 'FR' AND obs_cd IS NOT NULL AND obs_cd <> '' ORDER BY sgg_cd";
                DataTable dtDroughtCodes = await _dbService.GetDataTableAsync(droughtCodeQuery);
                _logger.Info($"처리 대상 유량 가뭄 코드 {dtDroughtCodes.Rows.Count} 건 로드", "FlowRateProcessor");

                if (dtDroughtCodes.Rows.Count == 0) return;

                foreach (DataRow row in dtDroughtCodes.Rows)
                {
                    string sggCode = StringUtils.ToStringSafe(row["sgg_cd"]);
                    string obsCodesCombined = StringUtils.ToStringSafe(row["obs_cd"]);
                    string[] observationSiteCodes = obsCodesCombined.Split('_'); // 관측소 코드

                    _logger.Info($"SGG 코드 [{sggCode}] (관측소 코드: {obsCodesCombined}) 처리 시작...", "FlowRateProcessor");

                    List<ValueDatePoint> dailyFlowData = await GetProcessedDailyFlowDataAsync(observationSiteCodes, sggCode);

                    // 1991년부터 마스터 종료일까지 데이터 생성
                    DateTime startDateForCsv = new DateTime(StartYear, 1, 1);
                    // endDateForCsv는 masterEndDate를 사용

                    _logger.Debug($"SGG 코드 [{sggCode}]: CSV 생성 범위: {startDateForCsv:yyyy-MM-dd} ~ {masterEndDate:yyyy-MM-dd}", "FlowRateProcessor");

                    List<ValueDatePoint> filledData = FillMissingDates(dailyFlowData, startDateForCsv, masterEndDate, -9999.0); // 결측치 -9999로 채움

                    await GenerateAndSaveCsvAsync(sggCode, filledData, masterEndDate);
                    await SaveToActualDroughtDamTableAsync(sggCode, filledData);
                }
            }
            catch (Exception ex)
            {
                _logger.Error("유량(FR) 데이터 처리 중 오류 발생", ex, "FlowRateProcessor");
            }
            _logger.Info("유량(FR) 데이터 처리 완료.", "FlowRateProcessor");
        }

        private async Task<List<ValueDatePoint>> GetProcessedDailyFlowDataAsync(string[] observationSiteCodes, string sggCodeContext)
        {
            var allFlowData = new List<WamisFlowDailyData>();
            foreach (string siteCode in observationSiteCodes)
            {
                string query = $"SELECT obscd, ymd, flow FROM public.tb_wamis_flowdtdata WHERE obscd = @obscd ORDER BY ymd";
                var parameters = new[] { new NpgsqlParameter("@obscd", siteCode) };
                DataTable dtRawData = await _dbService.GetDataTableAsync(query, parameters);

                foreach (DataRow row in dtRawData.Rows)
                {
                    string ymd = StringUtils.ToStringSafe(row["ymd"]);
                    double? flow = StringUtils.ToDoubleSafe(StringUtils.ToStringSafe(row["flow"]), -9999.0); // 기본 -9999

                    // JS_DAMRSRT의 지점별 날짜 필터링 로직 적용
                    DateTime currentDate = DateTimeUtils.ParseFromYyyyMMdd(ymd).GetValueOrDefault();
                    bool shouldSkip = false;
                    switch (sggCodeContext) // sgg_cd를 기준으로 필터링 (JS_DAMRSRT 참고)
                    {
                        case "42230": if (currentDate.Year < 2006 || currentDate > new DateTime(2020, 12, 31)) shouldSkip = true; break;
                        case "42800": if (currentDate.Year < 2010) shouldSkip = true; break;
                        case "47170": case "47760": if (currentDate.Year < 2000) shouldSkip = true; break;
                    }
                    if (shouldSkip) continue; // 필터링된 데이터는 제외

                    allFlowData.Add(new WamisFlowDailyData
                    {
                        ObservationSiteCode = StringUtils.ToStringSafe(row["obscd"]),
                        DateString = ymd,
                        FlowRate = flow == 0 ? -9999.0 : flow // JS_DAMRSRT는 0도 -9999로 처리하는 경향
                    });
                }
            }
            _logger.Debug($"총 {allFlowData.Count} 건의 유량 데이터 로드 (관측소: {string.Join(",", observationSiteCodes)})", "FlowRateProcessor");

            // 날짜별로 그룹화하고 유량 합산 (JS_DAMRSRT 로직)
            var dailyAggregatedData = allFlowData
                .GroupBy(d => d.DateString)
                .Select(g => new ValueDatePoint
                {
                    Date = DateTimeUtils.ParseFromYyyyMMdd(g.Key).Value,
                    Value = g.Sum(item => item.FlowRate ?? 0) // null이면 0으로 합산
                })
                .OrderBy(dp => dp.Date)
                .ToList();

            _logger.Debug($"{dailyAggregatedData.Count} 건의 일별 유량 데이터로 집계됨", "FlowRateProcessor");
            return dailyAggregatedData;
        }

        private List<ValueDatePoint> FillMissingDates(List<ValueDatePoint> existingData, DateTime startDate, DateTime endDate, double defaultValueForMissing)
        {
            var filledData = new List<ValueDatePoint>();
            var existingDataDict = existingData.ToDictionary(d => d.Date);

            for (DateTime currentDate = startDate; currentDate <= endDate; currentDate = currentDate.AddDays(1))
            {
                if (currentDate.Month == 2 && currentDate.Day == 29) continue;

                if (existingDataDict.TryGetValue(currentDate, out ValueDatePoint foundData))
                {
                    filledData.Add(new ValueDatePoint { Date = currentDate, Value = foundData.Value });
                }
                else
                {
                    filledData.Add(new ValueDatePoint { Date = currentDate, Value = defaultValueForMissing });
                }
            }
            return filledData;
        }

        private async Task GenerateAndSaveCsvAsync(string sggCode, List<ValueDatePoint> dataToSave, DateTime masterEndDate)
        {
            var csvLines = new List<string> { "yyyy,mm,dd,JD,Flow_Rate" };
            foreach (var dataPoint in dataToSave) // FillMissingDates에서 이미 전체 기간 채움
            {
                csvLines.Add(string.Format(CultureInfo.InvariantCulture, "{0},{1:D2},{2:D2},{3},{4}", // 유량은 F2 포맷 불필요할 수 있음
                    dataPoint.Date.Year, dataPoint.Date.Month, dataPoint.Date.Day,
                    DateTimeUtils.CalculateJulianDay(dataPoint.Date), dataPoint.Value.Value));
            }

            string csvFilePath = Path.Combine(_outputCsvDirectory, $"{sggCode}.csv");
            await File.WriteAllLinesAsync(csvFilePath, csvLines, Encoding.UTF8);
            _logger.Info($"SGG 코드 [{sggCode}] 유량(FR) CSV 파일 생성 완료: {csvFilePath} ({csvLines.Count - 1} 데이터 행, 최종일: {masterEndDate:yyyy-MM-dd})", "FlowRateProcessor");
        }

        private async Task SaveToActualDroughtDamTableAsync(string sggCode, List<ValueDatePoint> dataToSave)
        {
            var linesForDb = new List<string>();
            foreach (var dataPoint in dataToSave)
            {
                // JS_DAMRSRT의 SaveToDroughtTableWithCopy는 CSV와 동일한 모든 데이터를 저장하려고 시도.
                // -9999도 포함하여 저장하도록 수정. (단, BulkCopyFromCsvLinesAsync의 기본 파싱 로직에서 -9999가 null로 처리될 수 있으므로,
                // 해당 메소드에서 문자열 그대로 DB에 넣도록 하거나, DB 컬럼이 double이고 -9999를 그대로 넣을 수 있어야 함)
                // 여기서는 dataPoint.Value가 -9999.0일 수 있다고 가정하고 그대로 전달.
                linesForDb.Add(string.Format(CultureInfo.InvariantCulture, "{0},{1:D2},{2:D2},{3},{4}",
                    dataPoint.Date.Year, dataPoint.Date.Month, dataPoint.Date.Day,
                    DateTimeUtils.CalculateJulianDay(dataPoint.Date),
                    dataPoint.Value.HasValue ? dataPoint.Value.Value.ToString(CultureInfo.InvariantCulture) : "-9999" // DB 저장 시 문자열로 전달
                ));
            }

            if (!linesForDb.Any())
            {
                _logger.Info($"SGG 코드 [{sggCode}] (FR): tb_Actualdrought_DAM 테이블에 저장할 데이터 없음 (모든 기간이 결측일 수 있음).", "FlowRateProcessor");
                return;
            }

            var columnMapping = new List<Tuple<string, NpgsqlDbType>>
            {
                Tuple.Create("sgg_cd", NpgsqlDbType.Varchar),
                Tuple.Create("yyyy", NpgsqlDbType.Integer),
                Tuple.Create("mm", NpgsqlDbType.Integer),
                Tuple.Create("dd", NpgsqlDbType.Integer),
                Tuple.Create("jd", NpgsqlDbType.Integer),
                Tuple.Create("data", NpgsqlDbType.Double)
            };
            await _dbService.BulkCopyFromCsvLinesAsync("drought.tb_actualdrought_dam", sggCode, linesForDb, columnMapping);
            _logger.Info($"SGG 코드 [{sggCode}] (FR): tb_Actualdrought_DAM 테이블에 데이터 저장 완료 ({linesForDb.Count} 건).", "FlowRateProcessor");
        }
    }
}
