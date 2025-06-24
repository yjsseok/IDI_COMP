// DroughtDataProcessor/Program.cs
using System;
using System.Threading.Tasks;
using DroughtCore.Configuration;
using DroughtCore.Logging;
using DroughtCore.DataAccess;
using DroughtDataProcessor.Processors; // Processor 클래스들을 위한 네임스페이스

namespace DroughtDataProcessor
{
    class Program
    {
        // static GMLogManagerWrapper logger; // GMLogManager로 대체
        static ConfigManager configManager;
        static DbService dbService;

        static async Task Main(string[] args)
        {
            GMLogManager.Configure("log4net.config"); // log4net 설정
            GMLogManager.Info("DroughtDataProcessor Service 시작", "Program.Main");

            configManager = new ConfigManager(); // 로거 인자 없이 생성
            dbService = new DbService(configManager.Settings.ConnectionStrings.PostgreSqlConnection); // 로거 인자 없이 생성

            var areaRainfallProcessor = new AreaRainfallProcessor(dbService, configManager.Settings.OutputDirectories.AreaRainfallCsv); // 로거 인자 없이 생성
            await areaRainfallProcessor.ProcessDataAsync();

            var damRsrtProcessor = new DamRsrtProcessor(dbService, configManager.Settings.OutputDirectories.DamRsrtCsv);
            await damRsrtProcessor.ProcessDataAsync();

            var arDamProcessor = new ArDamProcessor(dbService, configManager.Settings.OutputDirectories.ArDamCsv);
            await arDamProcessor.ProcessDataAsync();

            var flowRateProcessor = new FlowRateProcessor(dbService, configManager.Settings.OutputDirectories.FlowRateCsv);
            await flowRateProcessor.ProcessDataAsync();

            var agAgProcessor = new AgAgProcessor(dbService, configManager.Settings.OutputDirectories.AgAgCsv);
            await agAgProcessor.ProcessDataAsync();
            await agAgProcessor.ExtendDiscontinuedAgDataAsync(configManager.Settings.OutputDirectories.AgAgCsv);

            GMLogManager.Info("DroughtDataProcessor Service 모든 작업 완료.", "Program.Main");
        }
    }
}
