// DroughtCore/Logging/GMLogManager.cs
using log4net;
using log4net.Config;
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace DroughtCore.Logging // 네임스페이스 변경
{
    public enum LogLevel // 로그 레벨 열거형 추가
    {
        Debug,
        Info,
        Warn,
        Error,
        Fatal
    }

    public static class GMLogManager
    {
        private static bool _isConfigured = false;
        private static readonly object _lock = new object();

        /// <summary>
        /// log4net 설정을 로드합니다. 프로그램 시작 시 한 번 호출해야 합니다.
        /// </summary>
        /// <param name="configFilePath">log4net 설정 파일 경로 (예: "log4net.config")</param>
        public static void Configure(string configFilePath = null)
        {
            lock (_lock)
            {
                if (!_isConfigured)
                {
                    if (string.IsNullOrEmpty(configFilePath))
                    {
                        // App.config 또는 web.config의 log4net 섹션을 사용하거나,
                        // 실행 파일과 동일한 디렉토리의 기본 설정 파일(log4net.config 등)을 찾도록 시도.
                        XmlConfigurator.Configure();
                        Console.WriteLine($"GMLogManager: log4net configured using default method (e.g., App.config or assembly attribute).");
                    }
                    else
                    {
                        if (!Path.IsPathRooted(configFilePath))
                        {
                            configFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, configFilePath);
                        }
                        if (File.Exists(configFilePath))
                        {
                            XmlConfigurator.Configure(new FileInfo(configFilePath));
                            Console.WriteLine($"GMLogManager: log4net configured using file: {configFilePath}");
                        }
                        else
                        {
                            // 설정 파일이 없으면 기본 콘솔 출력이라도 하도록 구성 시도 (선택적)
                            BasicConfigurator.Configure(); // 매우 기본적인 콘솔 로깅 설정
                            Console.WriteLine($"GMLogManager Warning: log4net config file not found at '{configFilePath}'. Basic console logging configured.");
                        }
                    }
                    _isConfigured = true;
                }
            }
        }

        private static ILog GetLogger(int stackFrameIndex = 2) // 스택 프레임 인덱스 조정
        {
            // Configure를 먼저 호출하도록 보장
            if (!_isConfigured) Configure();

            StackTrace st = new StackTrace();
            MethodBase method = st.GetFrame(stackFrameIndex)?.GetMethod(); // null 가능성 체크

            if (method == null || method.DeclaringType == null)
            {
                return LogManager.GetLogger("DefaultLogger"); // 비상용 로거
            }

            string methodName = method.Name;
            string declareTypeName = method.DeclaringType.Name;
            // string callingAssembly = method.DeclaringType.Assembly.FullName; // 너무 길어서 제외 고려
            // return LogManager.GetLogger(callingAssembly + " - " + declareTypeName + "." + methodName);
            return LogManager.GetLogger(declareTypeName + "." + methodName);
        }

        private static string FormatMessage(string message, string context = null)
        {
            string logMsg = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}]";
            if (!string.IsNullOrEmpty(context))
            {
                logMsg += $" [{context}]";
            }
            logMsg += $" {message}";
            return logMsg;
        }

        // ILogger 인터페이스와 유사한 메소드들 제공

        public static void Debug(string message, string context = null)
        {
            GetLogger(3).Debug(FormatMessage(message, context));
        }

        public static void Info(string message, string context = null)
        {
            GetLogger(3).Info(FormatMessage(message, context));
        }

        public static void Warn(string message, string context = null)
        {
            GetLogger(3).Warn(FormatMessage(message, context));
        }

        public static void Warn(string message, Exception ex, string context = null)
        {
            GetLogger(3).Warn(FormatMessage(message, context), ex);
        }

        public static void Error(string message, string context = null)
        {
            GetLogger(3).Error(FormatMessage(message, context));
        }

        public static void Error(string message, Exception ex, string context = null)
        {
            // GMLogHelper의 WriteLog(Exception ex)와 유사하게 호출 메소드 이름 포함
            MethodBase method = new StackTrace().GetFrame(1)?.GetMethod();
            string methodName = method != null ? method.Name : "UnknownMethod";
            string fullMessage = $"{FormatMessage(message, context)} (in method: {methodName})";
            GetLogger(3).Error(fullMessage, ex);
        }

        public static void Fatal(string message, string context = null)
        {
            GetLogger(3).Fatal(FormatMessage(message, context));
        }

        public static void Fatal(string message, Exception ex, string context = null)
        {
            MethodBase method = new StackTrace().GetFrame(1)?.GetMethod();
            string methodName = method != null ? method.Name : "UnknownMethod";
            string fullMessage = $"{FormatMessage(message, context)} (in method: {methodName})";
            GetLogger(3).Fatal(fullMessage, ex);
        }

        // 기존 WriteEntry 메소드들은 Info 레벨로 매핑하거나, 필요시 유지 (호환성)
        // 여기서는 Info 레벨로 매핑
        public static void WriteEntry(string message, string context = null)
        {
            Info(message, context); // GetLogger() 스택 깊이 조정을 위해 직접 호출
        }

        // EventLogEntryType 파라미터가 있는 WriteEntry들은 현재 log4net으로 직접 매핑하기 어려움.
        // EventLog에 직접 쓰는 로직이 필요하다면 별도 구현 또는 log4net의 EventLogAppender 사용.
        // 여기서는 메시지만 Info로 기록.
        public static void WriteEntry(string message, EventLogEntryType type, string context = null)
        {
            Info($"EventLogType [{type}]: {message}", context);
        }
        public static void WriteEntry(String message, EventLogEntryType type, int eventID, string context = null)
        {
            Info($"EventLogType [{type}], ID [{eventID}]: {message}", context);
        }
        // ... 기타 WriteEntry 오버로드도 유사하게 Info로 매핑 ...
    }
}
