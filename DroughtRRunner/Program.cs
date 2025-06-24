// DroughtRRunner/Program.cs
using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using DroughtCore.Configuration;
using DroughtCore.Logging;
using DroughtCore.DataAccess;
using DroughtCore.Models; // R 결과 저장을 위한 모델 (예: ROutputData)
using System.Collections.Generic;
using NpgsqlTypes; // For NpgsqlDbType
using System.Globalization; // For CultureInfo

namespace DroughtRRunner
{
    class Program
    {
        // static GMLogManagerWrapper logger; // GMLogManager로 대체
        static ConfigManager configManager;
        static DbService dbService;

        static async Task Main(string[] args)
        {
            GMLogManager.Configure("log4net.config");
            GMLogManager.Info("DroughtRRunner Service 시작", "Program.Main");

            configManager = new ConfigManager();
            dbService = new DbService(configManager.Settings.ConnectionStrings.PostgreSqlConnection);

            string rExecutable = configManager.Settings.RScriptSettings.RScriptExecutablePath;
            string baseScriptDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, configManager.Settings.RScriptSettings.BaseScriptPath);
            string mainRScript = Path.Combine(baseScriptDir, configManager.Settings.RScriptSettings.MainAnalysisScript);

            string inputCsvBaseDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "OutputData");

            string rOutputStorageDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, configManager.Settings.OutputDirectories.RScriptOutput);
            if (!Directory.Exists(rOutputStorageDir)) Directory.CreateDirectory(rOutputStorageDir);

            var rScriptRunner = new RScriptRunner(dbService, rExecutable); // 로거 인자 제거

            bool success = await rScriptRunner.ExecuteScriptAsync(mainRScript, inputCsvBaseDir, rOutputStorageDir);

            if(success)
            {
                GMLogManager.Info("R 스크립트 실행 성공 및 결과 처리 완료 (또는 시도됨).", "Program.Main");
            }
            else
            {
                GMLogManager.Error("R 스크립트 실행 또는 결과 처리에 실패했습니다.", "Program.Main");
            }

            GMLogManager.Info("DroughtRRunner Service 종료", "Program.Main");
        }
    }

    public class RScriptRunner
    {
        private readonly DbService _dbService;
        // private readonly ILogger _logger; // GMLogManager로 대체
        private readonly string _rscriptExecutablePath;

        public RScriptRunner(DbService dbService, string rscriptPath) // 로거 인자 제거
        {
            _dbService = dbService;
            // _logger = logger; // GMLogManager로 대체
            _rscriptExecutablePath = rscriptPath;
        }

        public async Task<bool> ExecuteScriptAsync(string scriptFilePath, string inputBaseDirectory, string outputBaseDirectory)
        {
            if (!File.Exists(_rscriptExecutablePath))
            {
                 GMLogManager.Error($"Rscript 실행 파일을 찾을 수 없습니다: '{_rscriptExecutablePath}'. PATH 환경변수를 확인하거나 설정 파일의 경로를 수정하십시오.", "RScriptRunner");
                return false;
            }
            if (!File.Exists(scriptFilePath))
            {
                GMLogManager.Error($"R 스크립트 파일을 찾을 수 없습니다: {scriptFilePath}", "RScriptRunner");
                return false;
            }
            if (!Directory.Exists(inputBaseDirectory))
            {
                 GMLogManager.Warn($"R 스크립트 입력 CSV 기본 디렉토리를 찾을 수 없습니다: {inputBaseDirectory}. 스크립트가 정상 동작하지 않을 수 있습니다.", "RScriptRunner");
            }
             if (!Directory.Exists(outputBaseDirectory))
            {
                try { Directory.CreateDirectory(outputBaseDirectory); }
                catch (Exception ex) {
                     GMLogManager.Error($"R 스크립트 결과물 저장 디렉토리 생성 실패: {outputBaseDirectory}", ex, "RScriptRunner");
                     return false;
                }
            }

            GMLogManager.Info($"R 스크립트 실행 시작: \"{_rscriptExecutablePath}\" \"{scriptFilePath}\" \"{inputBaseDirectory}\" \"{outputBaseDirectory}\"", "RScriptRunner");

            ProcessStartInfo startInfo = new ProcessStartInfo
            {
                FileName = _rscriptExecutablePath,
                Arguments = $"\"{scriptFilePath}\" \"{inputBaseDirectory}\" \"{outputBaseDirectory}\"",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            using (Process process = new Process { StartInfo = startInfo })
            {
                var outputBuilder = new System.Text.StringBuilder();
                var errorBuilder = new System.Text.StringBuilder();

                process.OutputDataReceived += (sender, e) => { if (e.Data != null) outputBuilder.AppendLine(e.Data); };
                process.ErrorDataReceived += (sender, e) => { if (e.Data != null) errorBuilder.AppendLine(e.Data); };

                try
                {
                    process.Start();
                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();

                    bool exited = await Task.Run(() => process.WaitForExit(30 * 60 * 1000));

                    if (!exited)
                    {
                        process.Kill();
                        GMLogManager.Error($"R 스크립트 실행 시간 초과 (30분). 프로세스를 강제 종료합니다. Script: {scriptFilePath}", "RScriptRunner");
                        return false;
                    }

                    string output = outputBuilder.ToString();
                    string error = errorBuilder.ToString();

                    if (process.ExitCode == 0)
                    {
                        GMLogManager.Info($"R 스크립트 실행 성공. 표준 출력:\n{output}", "RScriptRunner");
                        if (!string.IsNullOrWhiteSpace(error))
                        {
                             GMLogManager.Warn($"R 스크립트 실행 성공했으나 표준 오류 발생:\n{error}", "RScriptRunner");
                        }
                        return await ProcessROutputFilesAndSaveToDb(outputBaseDirectory);
                    }
                    else
                    {
                        GMLogManager.Error($"R 스크립트 실행 중 오류 발생 (Exit Code: {process.ExitCode}).\n표준 오류:\n{error}\n표준 출력:\n{output}", "RScriptRunner");
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    GMLogManager.Error($"R 스크립트 실행 중 예외 발생: {scriptFilePath}", ex, "RScriptRunner");
                    return false;
                }
            }
        }

        private async Task<bool> ProcessROutputFilesAndSaveToDb(string rOutputDirectory)
        {
            GMLogManager.Info($"R 실행 결과 처리 시작: {rOutputDirectory}", "RScriptRunner.DBStore");
            try
            {
                var resultFiles = Directory.GetFiles(rOutputDirectory, "r_output_*.csv");

                if (!resultFiles.Any())
                {
                    GMLogManager.Info("처리할 R 결과 파일이 없습니다.", "RScriptRunner.DBStore");
                    return true;
                }

                foreach (var filePath in resultFiles)
                {
                    GMLogManager.Info($"R 결과 파일 처리 중: {filePath}", "RScriptRunner.DBStore");
                    var linesToSave = new List<string>();
                    var fileLines = await File.ReadAllLinesAsync(filePath);

                    if (fileLines.Length <= 1)
                    {
                        GMLogManager.Warn($"R 결과 파일이 비어있거나 헤더만 있습니다: {filePath}", "RScriptRunner.DBStore");
                        continue;
                    }

                    for(int i=1; i < fileLines.Length; i++)
                    {
                        var parts = fileLines[i].Split(',');
                        if (parts.Length < 3) {
                             GMLogManager.Warn($"잘못된 형식의 R 결과 라인: '{fileLines[i]}' in {filePath}", "RScriptRunner.DBStore");
                            continue;
                        }

                        string sggCd = parts[0].Trim();
                        string dateStr = parts[1].Trim();
                        string indexValueStr = parts[2].Trim();

                        if (DateTime.TryParse(dateStr, out DateTime date) &&
                            double.TryParse(indexValueStr, NumberStyles.Any, CultureInfo.InvariantCulture, out double indexValue))
                        {
                            linesToSave.Add(string.Format(CultureInfo.InvariantCulture, "{0},{1:D2},{2:D2},{3},{4:F4}",
                                date.Year, date.Month, date.Day,
                                DateTimeUtils.CalculateJulianDay(date), indexValue));
                        }
                        else
                        {
                            GMLogManager.Warn($"R 결과 라인 파싱 실패: '{fileLines[i]}' in {filePath}", "RScriptRunner.DBStore");
                        }
                    }

                    if (linesToSave.Any())
                    {
                        var columnMapping = new List<Tuple<string, NpgsqlDbType>>
                        {
                            Tuple.Create("sgg_cd", NpgsqlDbType.Varchar),
                            Tuple.Create("yyyy", NpgsqlDbType.Integer),
                            Tuple.Create("mm", NpgsqlDbType.Integer),
                            Tuple.Create("dd", NpgsqlDbType.Integer),
                            Tuple.Create("jd", NpgsqlDbType.Integer),
                            Tuple.Create("r_index_value", NpgsqlDbType.Double)
                        };

                        string representativeSggCode = linesToSave.First().Split(',')[0];

                        await _dbService.BulkCopyFromCsvLinesAsync("drought.tb_r_script_results", representativeSggCode, linesToSave, columnMapping);
                        GMLogManager.Info($"R 결과 파일 {filePath}의 데이터 ({linesToSave.Count} 건) DB 저장 완료.", "RScriptRunner.DBStore");
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                GMLogManager.Error($"R 실행 결과 처리 중 오류 발생: {rOutputDirectory}", ex, "RScriptRunner.DBStore");
                return false;
            }
        }
    }
}
