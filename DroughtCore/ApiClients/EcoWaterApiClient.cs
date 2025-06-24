// DroughtCore/ApiClients/EcoWaterApiClient.cs
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using DroughtCore.Models;    // ApiModels (EcoWaterReservoirLevelItem 등)
using DroughtCore.Logging;  // ILogger
using System.Text.Json;     // System.Text.Json 또는 Newtonsoft.Json 선택

namespace DroughtCore.ApiClients
{
    // 농업용수공사(EcoWater) API 클라이언트 예시
    // 실제 API 명세 (기본 URL, 요청/응답 형식, 인증 방식 등)에 따라 수정 필요
    public class EcoWaterApiClient
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiKey; // 또는 다른 인증 정보
        private readonly ILogger _logger;
        private const string EcoWaterBaseUrl = "https://api.ekr.or.kr/"; // 예시 URL, 실제 API 기본 URL로 변경

        public EcoWaterApiClient(HttpClient httpClient, string apiKey, ILogger logger)
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _apiKey = apiKey; // API 키가 필요 없다면 제거하거나 다른 인증 방식으로 대체
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (_httpClient.BaseAddress == null)
            {
                _httpClient.BaseAddress = new Uri(EcoWaterBaseUrl);
            }
            // 필요한 경우 HttpClient 기본 헤더 설정 (예: 인증 토큰)
            // _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {_apiKey}");
        }

        /// <summary>
        /// 예시: 저수지 수위 현황 조회 API 호출
        /// (API 명세에 따라 메소드명, 파라미터, 반환 타입, 요청 URL, 파싱 로직 등 수정)
        /// </summary>
        /// <param name="facilityCode">시설 코드</param>
        /// <param name="date">조회 일자 (YYYYMMDD)</param>
        /// <returns></returns>
        public async Task<List<EcoWaterReservoirLevelItem>> GetReservoirLevelAsync(string facilityCode, string date)
        {
            // 실제 API 엔드포인트 및 요청 파라미터로 수정
            string requestUri = $"openapi/storagerate/list?fac_code={facilityCode}&stdt={date}&serviceKey={_apiKey}&type=json"; // JSON 응답 요청 예시

            _logger.Info($"EcoWater API 저수지 수위 요청 시작: {requestUri}", "EcoWaterApiClient");

            try
            {
                HttpResponseMessage response = await _httpClient.GetAsync(requestUri);
                response.EnsureSuccessStatusCode(); // 오류 시 예외 발생

                string responseBody = await response.Content.ReadAsStringAsync();
                _logger.Debug($"EcoWater API 응답 수신: {responseBody}", "EcoWaterApiClient");

                // JSON 응답 파싱 (System.Text.Json 예시)
                // 실제 응답 구조에 따라 EcoWaterResponse<EcoWaterReservoirLevelItem> 또는 직접 List<EcoWaterReservoirLevelItem> 등으로 파싱
                var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                var apiResponse = JsonSerializer.Deserialize<EcoWaterResponse<EcoWaterReservoirLevelItem>>(responseBody, options);
                // 또는 직접 리스트로 파싱: List<EcoWaterReservoirLevelItem> items = JsonSerializer.Deserialize<List<EcoWaterReservoirLevelItem>>(responseBody, options);

                if (apiResponse?.Items != null) // 실제 응답 구조에 맞춰 조건 변경
                {
                    _logger.Info($"EcoWater API 응답 성공: {apiResponse.Items.Count} 건 수신 (시설코드: {facilityCode})", "EcoWaterApiClient");
                    return apiResponse.Items;
                }
                else
                {
                    _logger.Warn($"EcoWater API 응답에서 유효한 데이터를 찾을 수 없거나 응답 형식이 예상과 다릅니다. (시설코드: {facilityCode})", "EcoWaterApiClient");
                    return new List<EcoWaterReservoirLevelItem>();
                }
            }
            catch (HttpRequestException httpEx)
            {
                _logger.Error($"EcoWater API HTTP 요청 오류 (시설코드: {facilityCode}, URI: {requestUri})", httpEx, "EcoWaterApiClient");
                throw;
            }
            catch (JsonException jsonEx)
            {
                _logger.Error($"EcoWater API JSON 파싱 오류 (시설코드: {facilityCode}, URI: {requestUri})", jsonEx, "EcoWaterApiClient");
                throw;
            }
            catch (Exception ex)
            {
                _logger.Error($"EcoWater API 알 수 없는 오류 (시설코드: {facilityCode}, URI: {requestUri})", ex, "EcoWaterApiClient");
                throw;
            }
        }
        // ... EcoWater API의 다른 필요한 메소드들 추가 ...
    }
}
