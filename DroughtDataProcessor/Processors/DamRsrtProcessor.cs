// DroughtDataProcessor/Processors/DamRsrtProcessor.cs
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
using DroughtCore.Models;    // DroughtCodeInfo, DamRawData, ActualDroughtDamData 등
using DroughtCore.Utils;    // DateTimeUtils, InterpolationUtils
using Npgsql;
using NpgsqlTypes;

namespace DroughtDataProcessor.Processors
{
    public class DamRsrtProcessor
    {
        private readonly DbService _dbService;
        private readonly ILogger _logger;
        private readonly string _outputCsvDirectory;
        private const int StartYear = 1991; // 요구사항: 1991년부터 데이터 생성

        public DamRsrtProcessor(DbService dbService, ILogger logger, string outputCsvDirectory)
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
            _logger.Info("댐 저수율 데이터 처리 시작...", "DamRsrtProcessor");
            try
            {
                // 1. drought_code 테이블에서 sort = 'Dam' 인 sgg_cd 및 obs_cd 목록 가져오기
                string droughtCodeQuery = "SELECT sgg_cd, obs_cd FROM public.drought_code WHERE sort = 'Dam' AND obs_cd IS NOT NULL AND obs_cd <> '' ORDER BY sgg_cd";
                DataTable dtDroughtCodes = await _dbService.GetDataTableAsync(droughtCodeQuery);
                _logger.Info($"처리 대상 댐 가뭄 코드 {dtDroughtCodes.Rows.Count} 건 로드", "DamRsrtProcessor");

                if (dtDroughtCodes.Rows.Count == 0) return;

                // 2. 각 sgg_cd에 대해 처리
                foreach (DataRow row in dtDroughtCodes.Rows)
                {
                    string sggCode = StringUtils.ToStringSafe(row["sgg_cd"]);
                    string obsCodesCombined = StringUtils.ToStringSafe(row["obs_cd"]);
                    string[] damCodes = obsCodesCombined.Split('_');

                    _logger.Info($"SGG 코드 [{sggCode}] (댐 코드: {obsCodesCombined}) 처리 시작...", "DamRsrtProcessor");

                    List<ValueDatePoint> dailyDamData = await GetProcessedDailyDamDataAsync(damCodes);

                    if (!dailyDamData.Any())
                    {
                        _logger.Warn($"SGG 코드 [{sggCode}]에 대한 처리 가능한 댐 데이터가 없습니다.", "DamRsrtProcessor");
                        continue;
                    }

                    // 1991년부터 데이터가 있는 마지막 날짜까지 데이터 생성 및 보간
                    // JS_DAMRSRT 방식: 데이터가 있는 기간 내에서만 채우고 보간
                    if (!dailyDamData.Any())
                    {
                        _logger.Warn($"SGG 코드 [{sggCode}]에 대한 처리 가능한 댐 데이터가 없어 CSV 및 DB 저장을 건너<0xEB><0x9B><0x84>니다.", "DamRsrtProcessor");
                        continue;
                    }
                    DateTime startDateForProcessing = dailyDamData.Min(d => d.Date);
                    DateTime endDateForProcessing = dailyDamData.Max(d => d.Date);

                    // "1991년부터" 요구사항과 "원래 방식" 사이의 절충:
                    // 데이터가 1991년 이후에 시작하면 그 시작일부터, 1991년 이전에 시작하면 1991년부터.
                    if (startDateForProcessing.Year >= StartYear)
                    {
                        // 데이터 시작일이 1991년 이후면, 그 시작일부터 사용
                    }
                    else
                    {
                         startDateForProcessing = new DateTime(StartYear, 1, 1);
                    }
                    // endDateForProcessing은 데이터의 마지막 날짜를 따르되, 오늘 이후는 생성 안함.
                    if (endDateForProcessing > DateTime.Today) endDateForProcessing = DateTime.Today;


                     _logger.Debug($"SGG 코드 [{sggCode}]: 원본 데이터 범위 {dailyDamData.Min(d=>d.Date):yyyy-MM-dd} ~ {dailyDamData.Max(d=>d.Date):yyyy-MM-dd}. 최종 처리 범위: {startDateForProcessing:yyyy-MM-dd} ~ {endDateForProcessing:yyyy-MM-dd}", "DamRsrtProcessor");

                    List<ValueDatePoint> filledAndInterpolatedData = FillAndInterpolate(dailyDamData, startDateForProcessing, endDateForProcessing, sggCode);

                    // CSV 파일 생성
                    await GenerateAndSaveCsvAsync(sggCode, filledAndInterpolatedData);

                    // (선택적) tb_Actualdrought_DAM 테이블에 저장
                    await SaveToActualDroughtDamTableAsync(sggCode, filledAndInterpolatedData);
                }
            }
            catch (Exception ex)
            {
                _logger.Error("댐 저수율 데이터 처리 중 오류 발생", ex, "DamRsrtProcessor");
            }
            _logger.Info("댐 저수율 데이터 처리 완료.", "DamRsrtProcessor");
        }

        private async Task<List<ValueDatePoint>> GetProcessedDailyDamDataAsync(string[] damCodes)
        {
            var allDamHourlyData = new List<DamRawData>();
            foreach (string damCode in damCodes)
            {
                // tb_wamis_mnhrdata 또는 DataCollector가 저장한 테이블에서 데이터 조회
                // obsh는 YYYYMMDDHH 형식의 문자열로 가정
                string query = $"SELECT damcd, obsh, rsrt FROM public.tb_wamis_mnhrdata WHERE damcd = @damcd AND rsrt IS NOT NULL AND rsrt <> -9999 ORDER BY obsh";
                var parameters = new[] { new NpgsqlParameter("@damcd", damCode) };
                DataTable dtRawData = await _dbService.GetDataTableAsync(query, parameters);

                foreach (DataRow row in dtRawData.Rows)
                {
                    string obsTimeString = StringUtils.ToStringSafe(row["obsh"]);
                    if (obsTimeString.Length == 10 && DateTime.TryParseExact(obsTimeString, "yyyyMMddHH", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime obsDateTime))
                    {
                        allDamHourlyData.Add(new DamRawData
                        {
                            DamCode = StringUtils.ToStringSafe(row["damcd"]),
                            ObservationDateTime = obsDateTime,
                            Obsh = obsTimeString,
                            ReservoirStorageRate = StringUtils.ToDoubleSafe(StringUtils.ToStringSafe(row["rsrt"]))
                        });
                    }
                }
            }
            _logger.Debug($"총 {allDamHourlyData.Count} 건의 시간별 댐 데이터 로드 (댐 코드: {string.Join(",", damCodes)})", "DamRsrtProcessor");


            // JS_DAMRSRT의 ProcessSggCds 로직 중 일별 데이터 가공 부분
            // (00시 데이터 무시, 24시는 다음날 데이터로, 그 외 시간은 해당일의 마지막 유효값 사용)
            var dailyAggregatedData = allDamHourlyData
                .Where(d => d.ObservationDateTime.Hour != 0) // 00시 데이터 제외
                .GroupBy(d => d.ObservationDateTime.Hour == 24 ? d.ObservationDateTime.Date.AddDays(1) : d.ObservationDateTime.Date)
                .Select(g => new ValueDatePoint
                {
                    Date = g.Key,
                    Value = g.OrderByDescending(item => item.ObservationDateTime.Hour) // 시간 역순 정렬
                               .Select(item => item.ReservoirStorageRate)
                               .FirstOrDefault(rsrt => rsrt.HasValue && rsrt.Value != 0 && rsrt.Value != -9999) // 0과 -9999 제외한 첫번째 유효값
                })
                .Where(dp => dp.Value.HasValue) // 유효한 값이 있는 날짜만 선택
                .OrderBy(dp => dp.Date)
                .ToList();

            _logger.Debug($"{dailyAggregatedData.Count} 건의 일별 댐 데이터로 집계됨", "DamRsrtProcessor");
            return dailyAggregatedData;
        }

        private List<ValueDatePoint> FillAndInterpolate(List<ValueDatePoint> existingData, DateTime startDate, DateTime endDate, string sggCodeLoggingContext)
        {
            var fullDateRangeData = new List<ValueDatePoint>();
            var existingDataDict = existingData.ToDictionary(d => d.Date);

            for (DateTime currentDate = startDate; currentDate <= endDate; currentDate = currentDate.AddDays(1))
            {
                if (currentDate.Month == 2 && currentDate.Day == 29) continue; // 윤달 2월 29일 제외

                if (existingDataDict.TryGetValue(currentDate, out ValueDatePoint foundData))
                {
                    fullDateRangeData.Add(new ValueDatePoint { Date = currentDate, Value = foundData.Value });
                }
                else
                {
                    fullDateRangeData.Add(new ValueDatePoint { Date = currentDate, Value = null }); // 결측치는 null로 시작
                }
            }
            _logger.Debug($"SGG [{sggCodeLoggingContext}]: {startDate:yyyy-MM-dd} ~ {endDate:yyyy-MM-dd} 범위의 {fullDateRangeData.Count}일 데이터 생성(결측 포함).", "DamRsrtProcessor");

            // 보간 처리 (JS_DAMRSRT는 0도 보간 대상이었음)
            return InterpolationUtils.LinearInterpolate(fullDateRangeData, 31, _logger, $"SGG[{sggCodeLoggingContext}]");
        }

        private async Task GenerateAndSaveCsvAsync(string sggCode, List<ValueDatePoint> dataToSave)
        {
            var csvLines = new List<string> { "yyyy,mm,dd,JD,RSRT" }; // 헤더
            foreach (var dataPoint in dataToSave.Where(dp => dp.Value.HasValue && dp.Value != 0)) // JS_DAMRSRT는 0이거나 null이면 기록 안함
            {
                // 보간된 값도 0일 수 있으므로, 여기서 0을 다시 필터링 할지 정책 필요.
                // JS_DAMRSRT는 보간 후에도 0/null이면 기록 안했음.
                if (!dataPoint.Value.HasValue || dataPoint.Value == 0) continue;


                csvLines.Add(string.Format(CultureInfo.InvariantCulture, "{0},{1:D2},{2:D2},{3},{4:F2}",
                    dataPoint.Date.Year,
                    dataPoint.Date.Month,
                    dataPoint.Date.Day,
                    DateTimeUtils.CalculateJulianDay(dataPoint.Date),
                    dataPoint.Value.Value
                ));
            }

            if (csvLines.Count <= 1) {
                _logger.Warn($"SGG 코드 [{sggCode}]에 대해 CSV로 저장할 유효한 데이터가 없습니다 (보간 후에도).", "DamRsrtProcessor");
                return;
            }

            string csvFilePath = Path.Combine(_outputCsvDirectory, $"{sggCode}.csv");
            await File.WriteAllLinesAsync(csvFilePath, csvLines, Encoding.UTF8);
            _logger.Info($"SGG 코드 [{sggCode}] 댐 저수율 CSV 파일 생성 완료: {csvFilePath} ({csvLines.Count - 1} 데이터 행)", "DamRsrtProcessor");
        }

        private async Task SaveToActualDroughtDamTableAsync(string sggCode, List<ValueDatePoint> dataToSave)
        {
            var linesForDb = new List<string>();
            foreach (var dataPoint in dataToSave.Where(dp => dp.Value.HasValue && dp.Value != 0)) // CSV와 동일한 조건
            {
                 if (!dataPoint.Value.HasValue || dataPoint.Value == 0) continue;

                linesForDb.Add(string.Format(CultureInfo.InvariantCulture, "{0},{1:D2},{2:D2},{3},{4:F2}",
                    dataPoint.Date.Year,
                    dataPoint.Date.Month,
                    dataPoint.Date.Day,
                    DateTimeUtils.CalculateJulianDay(dataPoint.Date),
                    dataPoint.Value.Value
                ));
            }

            if (!linesForDb.Any())
            {
                _logger.Info($"SGG 코드 [{sggCode}] : tb_Actualdrought_DAM 테이블에 저장할 데이터 없음.", "DamRsrtProcessor");
                return;
            }

            // 컬럼 매핑: sgg_cd, yyyy, mm, dd, jd, data
            var columnMapping = new List<Tuple<string, NpgsqlDbType>>
            {
                Tuple.Create("sgg_cd", NpgsqlDbType.Varchar),
                Tuple.Create("yyyy", NpgsqlDbType.Integer),
                Tuple.Create("mm", NpgsqlDbType.Integer),
                Tuple.Create("dd", NpgsqlDbType.Integer),
                Tuple.Create("jd", NpgsqlDbType.Integer),
                Tuple.Create("data", NpgsqlDbType.Double)
            };

            // 스키마명 'drought' 하드코딩. 설정으로 빼는 것 고려.
            await _dbService.BulkCopyFromCsvLinesAsync("drought.tb_actualdrought_dam", sggCode, linesForDb, columnMapping);
            _logger.Info($"SGG 코드 [{sggCode}] : tb_Actualdrought_DAM 테이블에 데이터 저장 완료 ({linesForDb.Count} 건).", "DamRsrtProcessor");
        }
    }
}
