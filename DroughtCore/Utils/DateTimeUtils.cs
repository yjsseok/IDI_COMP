// DroughtCore/Utils/DateTimeUtils.cs
using System;
using System.Globalization;

namespace DroughtCore.Utils
{
    public static class DateTimeUtils
    {
        /// <summary>
        /// JS_DAMRSRT 프로젝트의 CalculateJulianDay 메소드.
        /// 윤년의 2월 29일을 제외하고 Julian Day를 계산합니다. (1월 1일 = 1일)
        /// </summary>
        public static int CalculateJulianDay(DateTime date)
        {
            int dayOfYear = date.DayOfYear;
            // DateTime.IsLeapYear는 정확하지만, JS_DAMRSRT 로직은 단순히 2월 29일 이후면 1을 빼는 방식.
            // 해당 로직을 그대로 따르려면 date.Month > 2 조건만으로 충분할 수 있으나, 명확성을 위해 IsLeapYear 사용.
            if (DateTime.IsLeapYear(date.Year) && date.Month > 2)
            {
                dayOfYear--; // 2월 29일이 지난 경우, DayOfYear에서 1을 빼서 2월 29일을 건너뛴 효과
            }
            return dayOfYear;
        }

        /// <summary>
        /// 지정된 형식으로 날짜 문자열을 파싱합니다. 실패 시 null을 반환합니다.
        /// </summary>
        public static DateTime? ParseExactNullable(string dateString, string format, CultureInfo cultureInfo = null)
        {
            if (string.IsNullOrEmpty(dateString))
                return null;

            if (DateTime.TryParseExact(dateString, format, cultureInfo ?? CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
                return result;

            return null;
        }

        /// <summary>
        /// 지정된 날짜가 속한 월의 첫 날을 가져옵니다.
        /// </summary>
        public static DateTime GetFirstDayOfMonth(DateTime date)
        {
            return new DateTime(date.Year, date.Month, 1);
        }

        /// <summary>
        /// 지정된 날짜가 속한 월의 마지막 날을 가져옵니다.
        /// </summary>
        public static DateTime GetLastDayOfMonth(DateTime date)
        {
            return new DateTime(date.Year, date.Month, DateTime.DaysInMonth(date.Year, date.Month));
        }

        /// <summary>
        /// 다양한 표준 및 사용자 지정 형식으로 날짜를 문자열로 변환합니다.
        /// </summary>
        public static string FormatDate(DateTime date, string format = "yyyy-MM-dd")
        {
            return date.ToString(format);
        }

        // JS_DAMRSRT의 BaysDateTime.SingleFormatStringToDateTime 와 유사 기능
        /// <summary>
        /// "yyyyMMddHHmmss" 형식의 문자열을 DateTime으로 변환합니다.
        /// </summary>
        public static DateTime? ParseFromYyyyMMddHHmmss(string dateString)
        {
            if (string.IsNullOrWhiteSpace(dateString) || dateString.Length != 14)
                return null;

            if (DateTime.TryParseExact(dateString, "yyyyMMddHHmmss", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
                return result;

            return null;
        }
         /// <summary>
        /// "yyyyMMdd" 형식의 문자열을 DateTime으로 변환합니다.
        /// </summary>
        public static DateTime? ParseFromYyyyMMdd(string dateString)
        {
            if (string.IsNullOrWhiteSpace(dateString) || dateString.Length != 8)
                return null;

            return ParseExactNullable(dateString, "yyyyMMdd");
        }

    }

    public static class StringUtils
    {
        /// <summary>
        /// 객체가 null, DBNull 또는 빈 문자열인지 확인합니다.
        /// </summary>
        public static bool IsNullOrEmpty(object value)
        {
            return value == null || Convert.IsDBNull(value) || string.IsNullOrEmpty(value.ToString());
        }

        /// <summary>
        /// 객체를 문자열로 안전하게 변환합니다. null 또는 DBNull인 경우 기본값을 반환합니다.
        /// </summary>
        public static string ToStringSafe(object obj, string defaultValue = "")
        {
            return (obj == null || Convert.IsDBNull(obj)) ? defaultValue : obj.ToString();
        }

        /// <summary>
        /// 문자열을 정수로 안전하게 변환합니다. 실패 시 기본값을 반환합니다.
        /// </summary>
        public static int ToIntSafe(string s, int defaultValue = 0)
        {
            return int.TryParse(s, out int result) ? result : defaultValue;
        }

        /// <summary>
        /// 문자열을 double로 안전하게 변환합니다. 실패 시 기본값을 반환합니다.
        /// </summary>
        public static double ToDoubleSafe(string s, double defaultValue = 0.0)
        {
            return double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out double result) ? result : defaultValue;
        }

        /// <summary>
        /// HTML 태그를 제거합니다. (JS_DAMRSRT의 ToTagRemovedString 참고)
        /// </summary>
        public static string StripHtmlTags(string input)
        {
            if (string.IsNullOrEmpty(input)) return string.Empty;
            // 간단한 정규식. 더 강력한 HTML 파서가 필요할 수 있음.
            return System.Text.RegularExpressions.Regex.Replace(input, "<.*?>", string.Empty);
        }
    }
}
