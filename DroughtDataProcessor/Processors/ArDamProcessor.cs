// DroughtDataProcessor/Processors/ArDamProcessor.cs
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
using DroughtCore.Models;    // DroughtCodeInfo, ReservoirLevelData
using DroughtCore.Utils;    // DateTimeUtils, InterpolationUtils
using Npgsql;
using NpgsqlTypes;

namespace DroughtDataProcessor.Processors
{
    public class ArDamProcessor // 저수지 저수율 (Agricultural Reservoir)
    {
        private readonly DbService _dbService;
        private readonly ILogger _logger;
        private readonly string _outputCsvDirectory;
        private const int StartYear = 1991;

        public ArDamProcessor(DbService dbService, ILogger logger, string outputCsvDirectory)
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
            _logger.Info("저수지(AR) 저수율 데이터 처리 시작...", "ArDamProcessor");
            try
            {
                string droughtCodeQuery = "SELECT sgg_cd, obs_cd FROM public.drought_code WHERE sort = 'Ar' AND obs_cd IS NOT NULL AND obs_cd <> '' ORDER BY sgg_cd";
                DataTable dtDroughtCodes = await _dbService.GetDataTableAsync(droughtCodeQuery);
                _logger.Info($"처리 대상 저수지 가뭄 코드 {dtDroughtCodes.Rows.Count} 건 로드", "ArDamProcessor");

                if (dtDroughtCodes.Rows.Count == 0) return;

                foreach (DataRow row in dtDroughtCodes.Rows)
                {
                    string sggCode = StringUtils.ToStringSafe(row["sgg_cd"]);
                    string obsCodesCombined = StringUtils.ToStringSafe(row["obs_cd"]); // 시설 코드 (fac_code)
                    string[] facilityCodes = obsCodesCombined.Split('_');

                    _logger.Info($"SGG 코드 [{sggCode}] (시설 코드: {obsCodesCombined}) 처리 시작...", "ArDamProcessor");

                    List<ValueDatePoint> dailyReservoirData = await GetProcessedDailyReservoirDataAsync(facilityCodes);

                    if (!dailyReservoirData.Any())
                    {
                        _logger.Warn($"SGG 코드 [{sggCode}]에 대한 처리 가능한 저수지 데이터가 없습니다.", "ArDamProcessor");
                        continue;
                    }

                    // JS_DAMRSRT 방식: 데이터가 있는 기간 내에서만 채우고 보간
                    if (!dailyReservoirData.Any())
                    {
                        _logger.Warn($"SGG 코드 [{sggCode}]에 대한 처리 가능한 저수지 데이터가 없어 CSV 및 DB 저장을 건너<0xEB><0x9B><0x84>니다.", "ArDamProcessor");
                        continue;
                    }
                    DateTime startDateForProcessing = dailyReservoirData.Min(d => d.Date);
                    DateTime endDateForProcessing = dailyReservoirData.Max(d => d.Date);

                    if (startDateForProcessing.Year >= StartYear)
                    {
                        // 데이터 시작일이 1991년 이후면, 그 시작일부터 사용
                    }
                    else
                    {
                         startDateForProcessing = new DateTime(StartYear, 1, 1);
                    }
                    if (endDateForProcessing > DateTime.Today) endDateForProcessing = DateTime.Today;

                    _logger.Debug($"SGG 코드 [{sggCode}]: 원본 데이터 범위 {dailyReservoirData.Min(d=>d.Date):yyyy-MM-dd} ~ {dailyReservoirData.Max(d=>d.Date):yyyy-MM-dd}. 최종 처리 범위: {startDateForProcessing:yyyy-MM-dd} ~ {endDateForProcessing:yyyy-MM-dd}", "ArDamProcessor");

                    List<ValueDatePoint> filledAndInterpolatedData = FillAndInterpolate(dailyReservoirData, startDateForProcessing, endDateForProcessing, sggCode);

                    await GenerateAndSaveCsvAsync(sggCode, filledAndInterpolatedData);
                    await SaveToActualDroughtDamTableAsync(sggCode, filledAndInterpolatedData);
                }
            }
            catch (Exception ex)
            {
                _logger.Error("저수지(AR) 저수율 데이터 처리 중 오류 발생", ex, "ArDamProcessor");
            }
            _logger.Info("저수지(AR) 저수율 데이터 처리 완료.", "ArDamProcessor");
        }

        private async Task<List<ValueDatePoint>> GetProcessedDailyReservoirDataAsync(string[] facilityCodes)
        {
            var allReservoirData = new List<ReservoirLevelData>();
            foreach (string facCode in facilityCodes)
            {
                // tb_reserviorlevel 테이블에서 데이터 조회
                string query = $"SELECT fac_code, check_date, rate FROM public.tb_reserviorlevel WHERE fac_code = @fac_code AND rate IS NOT NULL AND rate <> -9999 ORDER BY check_date";
                var parameters = new[] { new NpgsqlParameter("@fac_code", facCode) };
                DataTable dtRawData = await _dbService.GetDataTableAsync(query, parameters);

                foreach (DataRow row in dtRawData.Rows)
                {
                    string checkDateString = StringUtils.ToStringSafe(row["check_date"]); // YYYYMMDD
                    if (checkDateString.Length == 8 && DateTime.TryParseExact(checkDateString, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime obsDate))
                    {
                        allReservoirData.Add(new ReservoirLevelData
                        {
                            FacilityCode = StringUtils.ToStringSafe(row["fac_code"]),
                            CheckDate = checkDateString,
                            StorageRate = StringUtils.ToDoubleSafe(StringUtils.ToStringSafe(row["rate"]))
                        });
                    }
                }
            }
             _logger.Debug($"총 {allReservoirData.Count} 건의 저수지 데이터 로드 (시설 코드: {string.Join(",", facilityCodes)})", "ArDamProcessor");

            // JS_DAMRSRT의 ProcessARdam 로직 중 일별 데이터 가공 부분
            // (하루 중 여러 값이 있으면 유효한 첫번째 값 사용 - 여기서는 이미 일별 데이터로 가정하고 가져옴)
            var dailyAggregatedData = allReservoirData
                .GroupBy(d => DateTimeUtils.ParseFromYyyyMMdd(d.CheckDate).Value) // 이미 일별 데이터
                .Select(g => new ValueDatePoint
                {
                    Date = g.Key,
                    Value = g.Select(item => item.StorageRate)
                               .FirstOrDefault(rate => rate.HasValue && rate.Value != 0 && rate.Value != -9999) // 0과 -9999 제외
                })
                .Where(dp => dp.Value.HasValue)
                .OrderBy(dp => dp.Date)
                .ToList();

            _logger.Debug($"{dailyAggregatedData.Count} 건의 일별 저수지 데이터로 집계됨", "ArDamProcessor");
            return dailyAggregatedData;
        }

        private List<ValueDatePoint> FillAndInterpolate(List<ValueDatePoint> existingData, DateTime startDate, DateTime endDate, string sggCodeLoggingContext)
        {
            var fullDateRangeData = new List<ValueDatePoint>();
            var existingDataDict = existingData.ToDictionary(d => d.Date);

            for (DateTime currentDate = startDate; currentDate <= endDate; currentDate = currentDate.AddDays(1))
            {
                if (currentDate.Month == 2 && currentDate.Day == 29) continue;

                if (existingDataDict.TryGetValue(currentDate, out ValueDatePoint foundData))
                {
                    fullDateRangeData.Add(new ValueDatePoint { Date = currentDate, Value = foundData.Value });
                }
                else
                {
                    fullDateRangeData.Add(new ValueDatePoint { Date = currentDate, Value = null });
                }
            }
            _logger.Debug($"SGG [{sggCodeLoggingContext}]: {startDate:yyyy-MM-dd} ~ {endDate:yyyy-MM-dd} 범위의 {fullDateRangeData.Count}일 데이터 생성(결측 포함).", "ArDamProcessor");

            return InterpolationUtils.LinearInterpolate(fullDateRangeData, 31, _logger, $"SGG[{sggCodeLoggingContext}]_AR");
        }

        private async Task GenerateAndSaveCsvAsync(string sggCode, List<ValueDatePoint> dataToSave)
        {
            var csvLines = new List<string> { "yyyy,mm,dd,JD,rsrt" }; // 헤더 (JS_DAMRSRT와 동일하게 rsrt로)
            foreach (var dataPoint in dataToSave.Where(dp => dp.Value.HasValue && dp.Value != 0))
            {
                 if (!dataPoint.Value.HasValue || dataPoint.Value == 0) continue;

                csvLines.Add(string.Format(CultureInfo.InvariantCulture, "{0},{1:D2},{2:D2},{3},{4:F2}",
                    dataPoint.Date.Year, dataPoint.Date.Month, dataPoint.Date.Day,
                    DateTimeUtils.CalculateJulianDay(dataPoint.Date), dataPoint.Value.Value));
            }

            if (csvLines.Count <= 1) {
                 _logger.Warn($"SGG 코드 [{sggCode}]에 대해 저수지(AR) CSV로 저장할 유효한 데이터가 없습니다.", "ArDamProcessor");
                return;
            }

            string csvFilePath = Path.Combine(_outputCsvDirectory, $"{sggCode}.csv");
            await File.WriteAllLinesAsync(csvFilePath, csvLines, Encoding.UTF8);
            _logger.Info($"SGG 코드 [{sggCode}] 저수지(AR) 저수율 CSV 파일 생성 완료: {csvFilePath} ({csvLines.Count - 1} 데이터 행)", "ArDamProcessor");
        }

        private async Task SaveToActualDroughtDamTableAsync(string sggCode, List<ValueDatePoint> dataToSave)
        {
            var linesForDb = new List<string>();
            foreach (var dataPoint in dataToSave.Where(dp => dp.Value.HasValue && dp.Value != 0))
            {
                if (!dataPoint.Value.HasValue || dataPoint.Value == 0) continue;
                linesForDb.Add(string.Format(CultureInfo.InvariantCulture, "{0},{1:D2},{2:D2},{3},{4:F2}",
                    dataPoint.Date.Year, dataPoint.Date.Month, dataPoint.Date.Day,
                    DateTimeUtils.CalculateJulianDay(dataPoint.Date), dataPoint.Value.Value));
            }

            if (!linesForDb.Any())
            {
                _logger.Info($"SGG 코드 [{sggCode}] (AR): tb_Actualdrought_DAM 테이블에 저장할 데이터 없음.", "ArDamProcessor");
                return;
            }

            var columnMapping = new List<Tuple<string, NpgsqlDbType>>
            {
                Tuple.Create("sgg_cd", NpgsqlDbType.Varchar),
                Tuple.Create("yyyy", NpgsqlDbType.Integer),
                Tuple.Create("mm", NpgsqlDbType.Integer),
                Tuple.Create("dd", NpgsqlDbType.Integer),
                Tuple.Create("jd", NpgsqlDbType.Integer),
                Tuple.Create("data", NpgsqlDbType.Double) // 'data' 컬럼 사용
            };
            await _dbService.BulkCopyFromCsvLinesAsync("drought.tb_actualdrought_dam", sggCode, linesForDb, columnMapping);
            _logger.Info($"SGG 코드 [{sggCode}] (AR): tb_Actualdrought_DAM 테이블에 데이터 저장 완료 ({linesForDb.Count} 건).", "ArDamProcessor");
        }
    }
}
