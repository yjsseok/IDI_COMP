// DroughtDataProcessor/Processors/AreaRainfallProcessor.cs
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
using DroughtCore.Models; // KmaAsosData, ThiessenPolygonData 등
using DroughtCore.Utils; // DateTimeUtils
using Npgsql; // NpgsqlParameter

namespace DroughtDataProcessor.Processors
{
    public class AreaRainfallProcessor
    {
        private readonly DbService _dbService;
        // private readonly ILogger _logger; // GMLogManager로 대체
        private readonly string _outputCsvDirectory;

        public AreaRainfallProcessor(DbService dbService, string outputCsvDirectory) // 로거 인자 제거
        {
            _dbService = dbService;
            // _logger = logger; // GMLogManager로 대체
            _outputCsvDirectory = outputCsvDirectory;
            if (!Directory.Exists(_outputCsvDirectory))
            {
                Directory.CreateDirectory(_outputCsvDirectory);
            }
        }

        public async Task ProcessDataAsync()
        {
            GMLogManager.Info("면적 강우 데이터 처리 시작...", "AreaRainfallProcessor");
            try
            {
                // 1. DB에서 원시 데이터 로드
                string rainfallQuery = "SELECT tm, stn, rn_day FROM public.tb_kma_asos_dtdata WHERE tm >= '19910101' ORDER BY tm, stn";
                DataTable dtRainfallData = await _dbService.GetDataTableAsync(rainfallQuery);
                GMLogManager.Info($"강우 데이터 로드: {dtRainfallData.Rows.Count} 건", "AreaRainfallProcessor");

                string thiessenQuery = "SELECT sgg_cd, code, ratio FROM public.tb_kma_asos_thiessen ORDER BY sgg_cd, code";
                DataTable dtThiessenPolygons = await _dbService.GetDataTableAsync(thiessenQuery);
                GMLogManager.Info($"티센 망 데이터 로드: {dtThiessenPolygons.Rows.Count} 건", "AreaRainfallProcessor");

                if (dtRainfallData.Rows.Count == 0 || dtThiessenPolygons.Rows.Count == 0)
                {
                    GMLogManager.Warn("강우 데이터 또는 티센 망 데이터가 없어 면적 강우 처리를 건너<0xEB><0x9B><0x84>니다.", "AreaRainfallProcessor");
                    return;
                }

                var thiessenMap = dtThiessenPolygons.AsEnumerable()
                    .GroupBy(row => row.Field<string>("sgg_cd"))
                    .ToDictionary(
                        g => g.Key,
                        g => g.Select(row => new ThiessenPolygonData {
                                SggCode = row.Field<string>("sgg_cd"),
                                StationCode = row.Field<string>("code"),
                                ThiessenRatio = row.Field<double>("ratio")
                            }).ToList()
                    );

                // 강우 데이터를 날짜(tm)와 지점(stn)으로 그룹화하여 빠르게 조회할 수 있도록 준비
                // 또는 날짜별로 순회하면서 해당 날짜의 모든 지점 강우량 처리
                var dailyStationRainfall = dtRainfallData.AsEnumerable()
                    .Select(row => new {
                        TmString = StringUtils.ToStringSafe(row["tm"]),
                        Station = StringUtils.ToStringSafe(row["stn"]),
                        Rainfall = StringUtils.ToDoubleSafe(StringUtils.ToStringSafe(row["rn_day"]), -9.0) // 결측치 -9로 가정
                    })
                    .Where(r => !string.IsNullOrEmpty(r.TmString) && r.TmString.Length >=8) // YYYYMMDD 형식 확인
                    .Select(r => new {
                        Date = DateTimeUtils.ParseFromYyyyMMdd(r.TmString.Substring(0,8)),
                        r.Station,
                        Rainfall = r.Rainfall == -9.0 ? 0.0 : r.Rainfall // 결측치 0으로 처리 (JS_DAMRSRT 방식)
                    })
                    .Where(r => r.Date.HasValue)
                    .GroupBy(r => r.Date.Value)
                    .ToDictionary(g => g.Key, g => g.ToDictionary(item => item.Station, item => item.Rainfall));


                foreach (var sggEntry in thiessenMap)
                {
                    string sggCode = sggEntry.Key;
                    List<ThiessenPolygonData> sggThiessenPolygons = sggEntry.Value;

                    _logger.Debug($"SGG 코드 [{sggCode}] 처리 시작. 티센 폴리곤 수: {sggThiessenPolygons.Count}", "AreaRainfallProcessor");

                    var csvLines = new List<string>();
                    // CSV 헤더: yyyy,MM,DD,JD,면적강우,관측소1코드_비율,관측소2코드_비율,...
                    var headerParts = new List<string> { "yyyy", "MM", "DD", "JD", "AreaRainfall" };
                    headerParts.AddRange(sggThiessenPolygons.Select(tp => $"{tp.StationCode}_{tp.ThiessenRatio:F4}")); // 비율 소수점 4자리로 포맷
                    csvLines.Add(string.Join(",", headerParts));

                    // 1991년 1월 1일부터 데이터가 있는 마지막 날짜까지 또는 현재까지 (정책 결정 필요)
                    // 여기서는 dailyStationRainfall에 있는 날짜만 처리
                    foreach (var dateEntry in dailyStationRainfall.OrderBy(de => de.Key))
                    {
                        DateTime currentDate = dateEntry.Key;
                        Dictionary<string, double> stationRainfallsForDate = dateEntry.Value;

                        if (currentDate.Month == 2 && currentDate.Day == 29) continue; // 윤달 2월 29일 제외

                        double calculatedAreaRainfall = 0;
                        var stationRainfallValuesForRow = new List<string>();

                        foreach (var thiessenPoly in sggThiessenPolygons)
                        {
                            double stationRain = stationRainfallsForDate.TryGetValue(thiessenPoly.StationCode, out var rain) ? rain : 0.0;
                            double ratio = thiessenPoly.ThiessenRatio;

                            // 174 지점 2011년 4월 1일 이전 데이터 비율 0 처리 (JS_DAMRSRT 로직)
                            if (thiessenPoly.StationCode == "174" && currentDate < new DateTime(2011, 4, 1))
                            {
                                ratio = 0;
                            }
                            calculatedAreaRainfall += stationRain * ratio;
                            stationRainfallValuesForRow.Add(stationRain.ToString("F1")); // 강우량 소수점 1자리
                        }

                        var lineParts = new List<string>
                        {
                            currentDate.Year.ToString(),
                            currentDate.Month.ToString("D2"),
                            currentDate.Day.ToString("D2"),
                            DateTimeUtils.CalculateJulianDay(currentDate).ToString(),
                            calculatedAreaRainfall.ToString("F2") // 면적강우 소수점 2자리
                        };
                        lineParts.AddRange(stationRainfallValuesForRow);
                        csvLines.Add(string.Join(",", lineParts));
                    }

                    string csvFilePath = Path.Combine(_outputCsvDirectory, $"{sggCode}.csv");
                    await File.WriteAllLinesAsync(csvFilePath, csvLines, Encoding.UTF8);
                    _logger.Info($"SGG 코드 [{sggCode}] 면적 강우 CSV 파일 생성 완료: {csvFilePath} ({csvLines.Count -1} 데이터 행)", "AreaRainfallProcessor");
                }
            }
            catch (Exception ex)
            {
                _logger.Error("면적 강우 데이터 처리 중 오류 발생", ex, "AreaRainfallProcessor");
            }
            _logger.Info("면적 강우 데이터 처리 완료.", "AreaRainfallProcessor");
        }
    }
}
