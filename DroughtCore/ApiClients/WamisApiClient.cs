// DroughtCore/ApiClients/WamisApiClient.cs
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Threading.Tasks;
using DroughtCore.Models;    // ApiModels 및 DbModels (DamRawData)
using DroughtCore.Logging;  // GMLogManager 사용
using U8Xml; // U8XmlParser NuGet 참조 필요

namespace DroughtCore.ApiClients
{
    public class WamisApiClient
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiKey;
        // GMLogManager는 정적 클래스이므로 멤버로 둘 필요 없음
        private const string WamisBaseUrl = "http://www.wamis.go.kr:8080/";

        public WamisApiClient(HttpClient httpClient, string apiKey) // 로거 인자 제거
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _apiKey = apiKey ?? throw new ArgumentNullException(nameof(apiKey));
            // GMLogManager.Configure(); // Configure는 Program.cs 등 앱 시작점에서 한 번만 호출

            if (_httpClient.BaseAddress == null)
            {
                _httpClient.BaseAddress = new Uri(WamisBaseUrl);
            }
        }

        /// <summary>
        /// WAMIS 댐 수문자료 API (시간자료) 호출 (mn_hrdata)
        /// </summary>
        public async Task<List<DamRawData>> GetDamHourlyDataAsync(string damCode, DateTime startDate, DateTime endDate)
        {
            string servicePath = "wamis/openapi/wkd/mn_hrdata";
            string requestUri = $"{servicePath}?damcd={damCode}&startdt={startDate:yyyyMMdd}&enddt={endDate:yyyyMMdd}&authKey={_apiKey}";

            GMLogManager.Info($"WAMIS 댐 시간자료 API 요청 시작: {requestUri}", "WamisApiClient");

            try
            {
                HttpResponseMessage response = await _httpClient.GetAsync(requestUri);
                response.EnsureSuccessStatusCode();

                using (var stream = await response.Content.ReadAsStreamAsync())
                using (XmlParser xml = await XmlParser.ParseAsync(stream))
                {
                    var items = new List<DamRawData>();
                    XmlNode root = xml.Root;

                    XmlNode listNode = root.Children.FirstOrDefault(n => n.Name == "list");
                    if (!listNode.IsEmpty)
                    {
                        foreach (XmlNode itemNode in listNode.Children)
                        {
                            if (itemNode.Name != "item") continue;

                            var damData = new DamRawData { DamCode = damCode };
                            bool isValidItem = true;

                            foreach(XmlNode fieldNode in itemNode.Children)
                            {
                                string fieldName = fieldNode.Name.ToString();
                                string fieldValue = fieldNode.InnerText.ToString();

                                switch (fieldName)
                                {
                                    case "damcd":
                                        damData.DamCode = fieldValue;
                                        break;
                                    case "obsdh":
                                        damData.Obsh = fieldValue;
                                        if (DateTime.TryParseExact(fieldValue, "yyyyMMddHH", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dt))
                                            damData.ObservationDateTime = dt;
                                        else
                                            GMLogManager.Warn($"날짜 파싱 실패 (obsdh): {fieldValue}", "WamisApiClient");
                                        break;
                                    case "swl":
                                        if (double.TryParse(fieldValue, out double swl)) damData.Swl = swl;
                                        else damData.Swl = null;
                                        break;
                                    case "rsrt":
                                        if (double.TryParse(fieldValue, out double rsrt)) damData.ReservoirStorageRate = rsrt;
                                        else damData.ReservoirStorageRate = null;
                                        break;
                                    case "inf":
                                        if (double.TryParse(fieldValue, out double inf)) damData.Inf = inf;
                                        else damData.Inf = null;
                                        break;
                                    case "tototf":
                                        if (double.TryParse(fieldValue, out double tototf)) damData.Tototf = tototf;
                                        else damData.Tototf = null;
                                        break;
                                    default:
                                        break;
                                }
                            }
                            if(isValidItem && !string.IsNullOrEmpty(damData.Obsh)) items.Add(damData);
                        }
                    }
                    else if (root.Name == "OpenAPI_ServiceResponse")
                    {
                        XmlNode cmmnMsgHeaderNode = root.Children.FirstOrDefault(n => n.Name == "cmmnMsgHeader");
                        if(!cmmnMsgHeaderNode.IsEmpty)
                        {
                            string returnReasonCode = cmmnMsgHeaderNode.Children.FirstOrDefault(n => n.Name == "returnReasonCode").InnerText.ToString();
                            string returnAuthMsg = cmmnMsgHeaderNode.Children.FirstOrDefault(n => n.Name == "returnAuthMsg").InnerText.ToString();
                            if (returnReasonCode != "00")
                            {
                                GMLogManager.Error($"WAMIS API 오류 응답: Code={returnReasonCode}, Msg={returnAuthMsg}", "WamisApiClient");
                                return new List<DamRawData>();
                            }
                        }
                    }

                    GMLogManager.Info($"WAMIS 댐 시간자료 API 응답 성공: {items.Count} 건 수신 (DamCode: {damCode})", "WamisApiClient");
                    return items;
                }
            }
            catch (HttpRequestException httpEx)
            {
                GMLogManager.Error($"WAMIS 댐 시간자료 API HTTP 요청 오류 (DamCode: {damCode}, URI: {requestUri})", httpEx, "WamisApiClient");
                throw;
            }
            catch (XmlException xmlEx)
            {
                GMLogManager.Error($"WAMIS 댐 시간자료 API XML 파싱 오류 (DamCode: {damCode}, URI: {requestUri})", xmlEx, "WamisApiClient");
                throw;
            }
            catch (Exception ex)
            {
                GMLogManager.Error($"WAMIS 댐 시간자료 API 알 수 없는 오류 (DamCode: {damCode}, URI: {requestUri})", ex, "WamisApiClient");
                throw;
            }
        }

        /// <summary>
        /// WAMIS 유량 일단위자료 API 호출 (flowdtd)
        /// </summary>
        public async Task<List<WamisFlowDailyData>> GetFlowDailyDataAsync(string observationSiteCode, DateTime startDate, DateTime endDate)
        {
            string servicePath = "wamis/openapi/wkd/flowdtd";
            string requestUri = $"{servicePath}?obscd={observationSiteCode}&startymd={startDate:yyyyMMdd}&endymd={endDate:yyyyMMdd}&authKey={_apiKey}";

            GMLogManager.Info($"WAMIS 유량 일단위자료 API 요청 시작: {requestUri}", "WamisApiClient");

            try
            {
                HttpResponseMessage response = await _httpClient.GetAsync(requestUri);
                response.EnsureSuccessStatusCode();

                using (var stream = await response.Content.ReadAsStreamAsync())
                using (XmlParser xml = await XmlParser.ParseAsync(stream))
                {
                    var items = new List<WamisFlowDailyData>();
                    XmlNode root = xml.Root;
                    XmlNode listNode = root.Children.FirstOrDefault(n => n.Name == "list");

                    if (!listNode.IsEmpty)
                    {
                        foreach (XmlNode itemNode in listNode.Children)
                        {
                            if (itemNode.Name != "item") continue;

                            var flowData = new WamisFlowDailyData { ObservationSiteCode = observationSiteCode };
                            foreach(XmlNode fieldNode in itemNode.Children)
                            {
                                string fieldName = fieldNode.Name.ToString();
                                string fieldValue = fieldNode.InnerText.ToString();
                                switch (fieldName)
                                {
                                    case "obscd":
                                        flowData.ObservationSiteCode = fieldValue;
                                        break;
                                    case "ymd":
                                        flowData.DateString = fieldValue;
                                        break;
                                    case "flow":
                                        if (double.TryParse(fieldValue, out double flow)) flowData.FlowRate = flow;
                                        else flowData.FlowRate = null;
                                        break;
                                }
                            }
                            if(!string.IsNullOrEmpty(flowData.DateString)) items.Add(flowData);
                        }
                    }
                     else if (root.Name == "OpenAPI_ServiceResponse")
                    {
                         GMLogManager.Error($"WAMIS 유량 API 오류 응답 (내용 확인 필요)", "WamisApiClient");
                         return new List<WamisFlowDailyData>();
                    }

                    GMLogManager.Info($"WAMIS 유량 일단위자료 API 응답 성공: {items.Count} 건 수신 (ObsCode: {observationSiteCode})", "WamisApiClient");
                    return items;
                }
            }
            catch (HttpRequestException httpEx)
            {
                GMLogManager.Error($"WAMIS 유량 일단위자료 API HTTP 요청 오류 (ObsCode: {observationSiteCode}, URI: {requestUri})", httpEx, "WamisApiClient");
                throw;
            }
            catch (XmlException xmlEx)
            {
                GMLogManager.Error($"WAMIS 유량 일단위자료 API XML 파싱 오류 (ObsCode: {observationSiteCode}, URI: {requestUri})", xmlEx, "WamisApiClient");
                throw;
            }
            catch (Exception ex)
            {
                GMLogManager.Error($"WAMIS 유량 일단위자료 API 알 수 없는 오류 (ObsCode: {observationSiteCode}, URI: {requestUri})", ex, "WamisApiClient");
                throw;
            }
        }

        // ... 기타 4개 API에 대한 클라이언트 클래스들 (EcoWaterApiClient.cs 등) ...
        // 각 API의 명세에 따라 요청 URL, 파라미터, 인증 방식, 응답 파싱 로직 구현.
        // 예: 농업용수공사(EcoWater) API 클라이언트 (tb_reserviorlevel 데이터 관련)
        // public class EcoWaterApiClient { ... }
    }
}
