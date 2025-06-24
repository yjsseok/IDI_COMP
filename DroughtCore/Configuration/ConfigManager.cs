// DroughtCore/Configuration/ConfigManager.cs
using System;
using Newtonsoft.Json; // JSON.NET 사용
using System.IO;
using DroughtCore.Logging; // GMLogManager 사용

namespace DroughtCore.Configuration
{
    public class AppSettings
    {
        public ConnectionStrings ConnectionStrings { get; set; }
        public ApiKeys ApiKeys { get; set; }
        public SchedulerSettings SchedulerSettings { get; set; }
        public LoggingSettings Logging { get; set; }
        public RScriptSettings RScriptSettings { get; set; }
        public OutputDirectories OutputDirectories { get; set; }
        public ExecutablePathsSettings ExecutablePaths { get; set; } // 실행 파일 경로 설정 추가
    }

    public class ExecutablePathsSettings // 새로운 설정 클래스
    {
        public string DataCollectorExePath { get; set; } = "DroughtDataCollector.exe"; // 기본값 또는 상대경로
        public string DataProcessorExePath { get; set; } = "DroughtDataProcessor.exe";
        public string RRunnerExePath { get; set; } = "DroughtRRunner.exe";
    }

    public class ConnectionStrings
    {
        public string PostgreSqlConnection { get; set; }
    }

    public class ApiKeys
    {
        public string WamisApiKey { get; set; }
        public string EcoWaterApiKey { get; set; }
        public string AnotherApiProviderKey1 { get; set; } // 나머지 3개 API 키 예시
        public string AnotherApiProviderKey2 { get; set; }
        public string AnotherApiProviderKey3 { get; set; }
    }

    public class SchedulerSettings
    {
        public int DataCollectionIntervalMinutes { get; set; } = 60;
        public int DataProcessingIntervalHours { get; set; } = 24;
        public string RScriptExecutionTime { get; set; } = "03:00";
    }

    public class LoggingSettings // GMLogManager 관련 설정 예시
    {
        public string GMLogManagerConfigPath { get; set; } = "Config/gmlog_config.xml";
        public string LogLevel { get; set; } = "Information"; // 예: Debug, Information, Warning, Error, Fatal
    }

    public class RScriptSettings
    {
        public string RScriptExecutablePath { get; set; } = "Rscript"; // 기본적으로 PATH에 설정된 Rscript 사용
        public string BaseScriptPath { get; set; } = "R_Scripts";
        public string MainAnalysisScript { get; set; } = "main_analysis.R";
    }

    public class OutputDirectories
    {
        public string AreaRainfallCsv { get; set; } = "OutputData/AreaRainfall";
        public string DamRsrtCsv { get; set; } = "OutputData/DamRsrt";
        public string ArDamCsv { get; set; } = "OutputData/ArDam";
        public string FlowRateCsv { get; set; } = "OutputData/FlowRate";
        public string AgAgCsv { get; set; } = "OutputData/AgAg";
        public string RScriptOutput { get; set; } = "OutputData/R_AnalysisResults";
    }


    public class ConfigManager
    {
        private const string DefaultConfigFileName = "appsettings.json";
        public AppSettings Settings { get; private set; }
        // GMLogManager는 정적 클래스이므로 멤버로 둘 필요 없음

        public ConfigManager(string configFilePath = null)
        {
            // GMLogManager.Configure(); // Configure는 Program.cs 등 앱 시작점에서 한 번만 호출
            string filePath = configFilePath ?? Path.Combine(AppDomain.CurrentDomain.BaseDirectory, DefaultConfigFileName);
            LoadConfig(filePath);
        }

        private void LoadConfig(string filePath)
        {
            try
            {
                if (!File.Exists(filePath))
                {
                    GMLogManager.Error($"설정 파일을 찾을 수 없습니다: {filePath}. 기본 설정을 생성하고 저장합니다.", "ConfigManager");
                    Settings = CreateDefaultSettings();
                    SaveConfig(filePath); // 기본 설정 파일 생성
                    return;
                }

                string jsonData = File.ReadAllText(filePath);
                Settings = JsonConvert.DeserializeObject<AppSettings>(jsonData);

                if (Settings == null)
                {
                    GMLogManager.Warn($"설정 파일 내용이 비어있거나 잘못되었습니다: {filePath}. 기본 설정을 사용합니다.", "ConfigManager");
                    Settings = CreateDefaultSettings();
                }
                else
                {
                    GMLogManager.Info($"설정 파일을 성공적으로 로드했습니다: {filePath}", "ConfigManager");
                }
            }
            catch (JsonException jsonEx)
            {
                 GMLogManager.Error($"설정 파일 JSON 파싱 중 오류 발생: {filePath}", jsonEx, "ConfigManager");
                Settings = CreateDefaultSettings();
            }
            catch (Exception ex)
            {
                GMLogManager.Error($"설정 파일 로드 중 오류 발생: {filePath}", ex, "ConfigManager");
                Settings = CreateDefaultSettings();
            }
        }

        private AppSettings CreateDefaultSettings()
        {
            GMLogManager.Info("기본 설정 객체를 생성합니다.", "ConfigManager");
            return new AppSettings
            {
                ConnectionStrings = new ConnectionStrings { PostgreSqlConnection = "Server=YOUR_SERVER;Port=5432;Database=YOUR_DB;User Id=YOUR_USER;Password=YOUR_PASSWORD;SearchPath=public" },
                ApiKeys = new ApiKeys {
                    WamisApiKey = "YOUR_WAMIS_API_KEY",
                    EcoWaterApiKey = "YOUR_ECOWATER_API_KEY",
                    AnotherApiProviderKey1 = "KEY1",
                    AnotherApiProviderKey2 = "KEY2",
                    AnotherApiProviderKey3 = "KEY3"
                },
                SchedulerSettings = new SchedulerSettings(),
                Logging = new LoggingSettings(),
                RScriptSettings = new RScriptSettings(),
                OutputDirectories = new OutputDirectories()
            };
        }

        public void SaveConfig(string filePath = null)
        {
            filePath = filePath ?? Path.Combine(AppDomain.CurrentDomain.BaseDirectory, DefaultConfigFileName);
            try
            {
                string jsonData = JsonConvert.SerializeObject(Settings, Formatting.Indented);
                File.WriteAllText(filePath, jsonData);
                GMLogManager.Info($"설정이 파일에 저장되었습니다: {filePath}", "ConfigManager");
            }
            catch (Exception ex)
            {
                GMLogManager.Error($"설정 파일 저장 중 오류 발생: {filePath}", ex, "ConfigManager");
            }
        }
    }
}
