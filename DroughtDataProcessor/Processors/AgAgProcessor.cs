// DroughtDataProcessor/Processors/AgAgProcessor.cs
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
using DroughtCore.Models;    // ReservoirLevelData
using DroughtCore.Utils;    // DateTimeUtils, InterpolationUtils
using Npgsql;
using NpgsqlTypes; // 필요시 DbService에서 사용

namespace DroughtDataProcessor.Processors
{
    public class AgAgProcessor // 농업용수 저수율
    {
        private readonly DbService _dbService;
        private readonly ILogger _logger;
        private readonly string _outputCsvDirectory;
        private const int StartYear = 1991;

        // JS_DAMRSRT의 ExtendDiscontinuedDataAsync에서 사용된 지원 종료 지역 코드 목록
        private readonly List<string> _discontinuedFacilityCodes = new List<string>
        {
            "2914010008", "2917010030", "2920010054", "2920010055", "4113010002",
            "4153010009", "4315010003", "4315010022", "4377010043", "4423010044",
            "4574010038", "4672010106", "4683010147", "4684010186", "4711010035",
            "4775010157", "4783010039", "4783010042", "4825010142"
        };


        public AgAgProcessor(DbService dbService, ILogger logger, string outputCsvDirectory)
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
            _logger.Info("농업용수(AgAG) 저수율 데이터 처리 시작...", "AgAgProcessor");
            try
            {
                // 1. tb_reserviorlevel 테이블에서 모든 fac_code 목록 가져오기
                string facCodeQuery = "SELECT DISTINCT fac_code FROM public.tb_reserviorlevel WHERE fac_code IS NOT NULL AND fac_code <> '' ORDER BY fac_code";
                DataTable dtFacCodes = await _dbService.GetDataTableAsync(facCodeQuery);
                _logger.Info($"처리 대상 농업용 저수지 시설 코드 {dtFacCodes.Rows.Count} 건 로드", "AgAgProcessor");

                if (dtFacCodes.Rows.Count == 0) return;

                foreach (DataRow row in dtFacCodes.Rows)
                {
                    string facilityCode = StringUtils.ToStringSafe(row["fac_code"]);
                    _logger.Info($"시설 코드 [{facilityCode}] 처리 시작...", "AgAgProcessor");

                    List<ValueDatePoint> dailyAgData = await GetProcessedDailyAgDataAsync(facilityCode);

                    if (!dailyAgData.Any())
                    {
                        _logger.Warn($"시설 코드 [{facilityCode}]에 대한 처리 가능한 데이터가 없습니다.", "AgAgProcessor");
                        // JS_DAMRSRT에서는 데이터 없어도 1991년부터 마지막 날짜까지 -9999로 채워서 CSV 생성했었음.
                        // 이 정책을 따르려면 빈 dailyAgData로 FillAndInterpolate 호출
                    }

                    // JS_DAMRSRT 방식: 1991년부터 데이터가 있는 마지막 날짜까지 채움.
                    DateTime startDateForProcessing = new DateTime(StartYear, 1, 1);
                    DateTime endDateForProcessing;

                    if (!dailyAgData.Any())
                    {
                        // 데이터가 전혀 없는 경우, JS_DAMRSRT는 Procsee_AG에서 rawData.Last().Date 접근 시 오류 발생 가능성 있음.
                        // 여기서는 1991년 1월 1일부터 오늘까지 -9999로 채워진 CSV를 만들도록 처리 (ExtendDiscontinuedAgDataAsync와 유사)
                        // 또는 아예 CSV를 생성하지 않도록 할 수도 있음. JS_DAMRSRT는 빈 파일이라도 만들었을 수 있음.
                        // 여기서는 오늘까지 -9999로 채우는 것으로 가정.
                        _logger.Warn($"시설 코드 [{facilityCode}]에 대한 원본 데이터가 전혀 없습니다. 1991년부터 오늘까지 -9999로 CSV를 생성합니다.", "AgAgProcessor");
                        endDateForProcessing = DateTime.Today;
                    }
                    else
                    {
                        endDateForProcessing = dailyAgData.Max(d => d.Date);
                        if (endDateForProcessing > DateTime.Today) endDateForProcessing = DateTime.Today;
                    }

                    // 데이터 시작일이 1991년 이전이면 1991년부터, 아니면 데이터 시작일부터 (단, 항상 1991년부터 채우도록 startDateForProcessing 고정)
                    // JS_DAMRSRT는 rawData.Last().Date를 endDate로 사용하고 startDate는 1991년으로 고정했음.

                    _logger.Debug($"시설 코드 [{facilityCode}]: 원본 데이터 최대일자 {endDateForProcessing:yyyy-MM-dd}. CSV 생성 범위: {startDateForProcessing:yyyy-MM-dd} ~ {endDateForProcessing:yyyy-MM-dd}", "AgAgProcessor");

                    List<ValueDatePoint> filledAndInterpolatedData = FillAndInterpolateAg(dailyAgData, startDateForProcessing, endDateForProcessing, facilityCode);

                    await GenerateAndSaveCsvAsync(facilityCode, filledAndInterpolatedData);
                    // AgAG 데이터는 tb_Actualdrought_DAM 테이블에 저장하지 않음 (JS_DAMRSRT 기준)
                }
            }
            catch (Exception ex)
            {
                _logger.Error("농업용수(AgAG) 저수율 데이터 처리 중 오류 발생", ex, "AgAgProcessor");
            }
            _logger.Info("농업용수(AgAG) 저수율 데이터 처리 완료.", "AgAgProcessor");
        }

        private async Task<List<ValueDatePoint>> GetProcessedDailyAgDataAsync(string facilityCode)
        {
            var reservoirData = new List<ReservoirLevelData>();
            string query = $"SELECT fac_code, check_date, rate FROM public.tb_reserviorlevel WHERE fac_code = @fac_code AND rate IS NOT NULL AND rate <> '' AND rate <> '-9999' ORDER BY check_date"; // -9999도 유효하지 않은 값으로 처리
            var parameters = new[] { new NpgsqlParameter("@fac_code", facilityCode) };
            DataTable dtRawData = await _dbService.GetDataTableAsync(query, parameters);

            foreach (DataRow row in dtRawData.Rows)
            {
                string checkDateString = StringUtils.ToStringSafe(row["check_date"]);
                if (checkDateString.Length == 8 && DateTime.TryParseExact(checkDateString, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime obsDate))
                {
                    reservoirData.Add(new ReservoirLevelData
                    {
                        FacilityCode = StringUtils.ToStringSafe(row["fac_code"]),
                        CheckDate = checkDateString,
                        StorageRate = StringUtils.ToDoubleSafe(StringUtils.ToStringSafe(row["rate"]))
                    });
                }
            }
            _logger.Debug($"총 {reservoirData.Count} 건의 농업용수 데이터 로드 (시설 코드: {facilityCode})", "AgAgProcessor");

            // JS_DAMRSRT의 Procsee_AG는 이미 일별 데이터로 가정하고, rate가 -9999가 아닌 것만 사용.
            // 여기서는 이미 쿼리에서 필터링. 추가적인 일별 집계 로직은 불필요.
            return reservoirData
                .Select(d => new ValueDatePoint { Date = DateTimeUtils.ParseFromYyyyMMdd(d.CheckDate).Value, Value = d.StorageRate })
                .OrderBy(dp => dp.Date)
                .ToList();
        }

        private List<ValueDatePoint> FillAndInterpolateAg(List<ValueDatePoint> existingData, DateTime startDate, DateTime endDate, string facilityCodeLoggingContext)
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
                    fullDateRangeData.Add(new ValueDatePoint { Date = currentDate, Value = null }); // 결측치는 null로 시작
                }
            }
            _logger.Debug($"시설 [{facilityCodeLoggingContext}]: {startDate:yyyy-MM-dd} ~ {endDate:yyyy-MM-dd} 범위의 {fullDateRangeData.Count}일 데이터 생성(결측 포함).", "AgAgProcessor");

            // 보간 (JS_DAMRSRT의 Procsee_AG 내부 보간 로직 참고)
            // 앞/뒤 31일, 그래도 없으면 -9999 (여기서는 null 유지 후 CSV 생성 시 -9999로)
            var interpolated = InterpolationUtils.LinearInterpolate(fullDateRangeData, 31, _logger, $"Facility[{facilityCodeLoggingContext}]_AgAG");

            // 보간 후에도 null인 값은 -9999로 채우지 않고 null 유지 (CSV 생성 시 처리)
            return interpolated;
        }

        private async Task GenerateAndSaveCsvAsync(string facilityCode, List<ValueDatePoint> dataToSave)
        {
            var csvLines = new List<string> { "yyyy,mm,dd,JD,rate" }; // 헤더
            foreach (var dataPoint in dataToSave)
            {
                // JS_DAMRSRT의 Procsee_AG는 보간 후에도 null이면 -9999로 CSV에 썼음.
                string valueToWrite = dataPoint.Value.HasValue ? dataPoint.Value.Value.ToString("F2", CultureInfo.InvariantCulture) : "-9999";

                csvLines.Add(string.Format(CultureInfo.InvariantCulture, "{0},{1:D2},{2:D2},{3},{4}",
                    dataPoint.Date.Year, dataPoint.Date.Month, dataPoint.Date.Day,
                    DateTimeUtils.CalculateJulianDay(dataPoint.Date), valueToWrite));
            }

            string csvFilePath = Path.Combine(_outputCsvDirectory, $"{facilityCode}.csv");
            await File.WriteAllLinesAsync(csvFilePath, csvLines, Encoding.UTF8);
            _logger.Info($"시설 코드 [{facilityCode}] 농업용수(AgAG) CSV 파일 생성 완료: {csvFilePath} ({csvLines.Count - 1} 데이터 행)", "AgAgProcessor");
        }

        public async Task ExtendDiscontinuedAgDataAsync(string agagCsvBaseDirectory)
        {
            _logger.Info("지원 종료 지역 농업용수 데이터 확장 작업 시작...", "AgAgProcessor_Extend");
            if (!Directory.Exists(agagCsvBaseDirectory))
            {
                _logger.Error($"AgAG CSV 폴더를 찾을 수 없습니다: {agagCsvBaseDirectory}", "AgAgProcessor_Extend");
                return;
            }

            try
            {
                DateTime masterEndDate = DateTime.MinValue;
                var allCsvFiles = Directory.GetFiles(agagCsvBaseDirectory, "*.csv");

                foreach (var file in allCsvFiles) // 모든 파일 중 가장 마지막 날짜 찾기
                {
                    var lines = await File.ReadAllLinesAsync(file);
                    if (lines.Length > 1) // 헤더 제외
                    {
                        var lastLineParts = lines.Last().Split(',');
                        if (lastLineParts.Length >= 3 &&
                            DateTime.TryParseExact($"{lastLineParts[0]}{lastLineParts[1]}{lastLineParts[2]}", "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime currentDate))
                        {
                            if (currentDate > masterEndDate) masterEndDate = currentDate;
                        }
                    }
                }

                if (masterEndDate == DateTime.MinValue)
                {
                    _logger.Warn("AgAG 데이터의 최종 기준일을 찾을 수 없어 확장 작업을 건너<0xEB><0x9B><0x84>니다.", "AgAgProcessor_Extend");
                    return;
                }
                 _logger.Info($"AgAG 데이터 최종 기준일: {masterEndDate:yyyy-MM-dd}", "AgAgProcessor_Extend");

                foreach (var facilityCode in _discontinuedFacilityCodes)
                {
                    string filePath = Path.Combine(agagCsvBaseDirectory, $"{facilityCode}.csv");
                    if (!File.Exists(filePath))
                    {
                        _logger.Warn($"지원 종료 대상 파일 없음: {facilityCode}.csv. 빈 파일 생성 후 확장합니다.", "AgAgProcessor_Extend");
                        // 빈 파일을 만들고 헤더만 써줌.
                        await File.WriteAllTextAsync(filePath, "yyyy,mm,dd,JD,rate" + Environment.NewLine);
                    }

                    var lines = (await File.ReadAllLinesAsync(filePath)).ToList();
                    DateTime stationLastDate = new DateTime(StartYear-1,1,1); // 매우 작은 값으로 초기화

                    if (lines.Count > 1)
                    {
                         var lastLineParts = lines.Last().Split(',');
                         if (lastLineParts.Length >=3 && DateTime.TryParseExact($"{lastLineParts[0]}{lastLineParts[1]}{lastLineParts[2]}", "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDate))
                         {
                            stationLastDate = parsedDate;
                         }
                    } else { // 헤더만 있는 경우, 시작일부터 채워야 함
                        stationLastDate = new DateTime(StartYear, 1, 1).AddDays(-1) ; // 첫날부터 채우도록 조정
                    }


                    if (stationLastDate >= masterEndDate)
                    {
                        _logger.Info($"시설 코드 [{facilityCode}]은 이미 최신 상태({stationLastDate:yyyy-MM-dd})입니다. 확장 건너<0xEB><0x9B><0x84>니다.", "AgAgProcessor_Extend");
                        continue;
                    }

                    _logger.Info($"시설 코드 [{facilityCode}] 데이터 확장 중... ({stationLastDate:yyyy-MM-dd} -> {masterEndDate:yyyy-MM-dd})", "AgAgProcessor_Extend");

                    var linesToAdd = new StringBuilder();
                    for (DateTime date = stationLastDate.AddDays(1); date <= masterEndDate; date = date.AddDays(1))
                    {
                        if (date.Month == 2 && date.Day == 29) continue;
                        linesToAdd.AppendLine($"{date.Year},{date.Month:D2},{date.Day:D2},{DateTimeUtils.CalculateJulianDay(date)},-9999");
                    }
                    await File.AppendAllTextAsync(filePath, linesToAdd.ToString());
                    _logger.Info($"시설 코드 [{facilityCode}] 데이터 확장 완료.", "AgAgProcessor_Extend");
                }
            }
            catch (Exception ex)
            {
                _logger.Error("지원 종료 지역 농업용수 데이터 확장 중 오류 발생", ex, "AgAgProcessor_Extend");
            }
            _logger.Info("지원 종료 지역 농업용수 데이터 확장 작업 완료.", "AgAgProcessor_Extend");
        }
    }
}
