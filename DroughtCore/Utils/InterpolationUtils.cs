// DroughtCore/Utils/InterpolationUtils.cs
using System;
using System.Collections.Generic;
using System.Linq;
using DroughtCore.Models; // 예시: 보간 대상 데이터 모델 (ValueDatePoint 등)
using DroughtCore.Logging;

namespace DroughtCore.Utils
{
    public class ValueDatePoint
    {
        public DateTime Date { get; set; }
        public double? Value { get; set; }
        public bool Interpolated { get; set; } = false; // 보간된 값인지 여부
    }

    public static class InterpolationUtils
    {
        /// <summary>
        /// JS_DAMRSRT 프로젝트의 댐/저수지 저수율 보간 로직을 일반화한 함수.
        /// 결측치 또는 0인 값을 앞/뒤 31일 이내의 유효한 값들의 평균으로 선형 보간합니다.
        /// </summary>
        /// <param name="dataPoints">날짜 오름차순으로 정렬된 ValueDatePoint 리스트</param>
        /// <param name="interpolationWindowDays">보간을 위해 앞/뒤로 탐색할 최대 일수</param>
        /// <param name="logger">로깅을 위한 로거 인스턴스</param>
        /// <param name="context">로깅 컨텍스트 (예: 댐 코드, 저수지 코드)</param>
        /// <returns>보간 처리된 ValueDatePoint 리스트</returns>
        public static List<ValueDatePoint> LinearInterpolate(List<ValueDatePoint> dataPoints, int interpolationWindowDays = 31, ILogger logger = null, string context = null)
        {
            if (dataPoints == null || !dataPoints.Any())
            {
                return new List<ValueDatePoint>();
            }

            var interpolatedList = dataPoints.Select(dp => new ValueDatePoint { Date = dp.Date, Value = dp.Value, Interpolated = dp.Interpolated }).ToList(); // 원본 수정을 피하기 위해 복사본 사용

            for (int i = 0; i < interpolatedList.Count; i++)
            {
                if (interpolatedList[i].Value == null || interpolatedList[i].Value == 0) // JS_DAMRSRT에서는 0도 결측치로 간주하여 보간
                {
                    DateTime currentDate = interpolatedList[i].Date;
                    double? closestBeforeValue = null;
                    double? closestAfterValue = null;
                    int daysToClosestBefore = int.MaxValue;
                    int daysToClosestAfter = int.MaxValue;

                    // 1. 앞쪽 N일 이내 가장 가까운 유효값 찾기
                    for (int j = i - 1; j >= 0; j--)
                    {
                        int diffDays = (currentDate - interpolatedList[j].Date).Days;
                        if (diffDays > interpolationWindowDays) break;

                        if (interpolatedList[j].Value != null && interpolatedList[j].Value != 0)
                        {
                            // 가장 가까운 '유효한' 값을 찾아야 함. diffDays가 작은 것을 우선.
                            // JS_DAMRSRT에서는 가장 가까운 하나만 찾음.
                            if (diffDays < daysToClosestBefore) { // 이 조건은 항상 참이 아닐 수 있음. 처음 발견된 유효값을 사용.
                                closestBeforeValue = interpolatedList[j].Value;
                                daysToClosestBefore = diffDays; // 실제 경과일 기록
                                break; // 가장 가까운 하나를 찾으면 중단 (JS_DAMRSRT 방식)
                            }
                        }
                    }

                    // 2. 뒤쪽 N일 이내 가장 가까운 유효값 찾기
                    for (int j = i + 1; j < interpolatedList.Count; j++)
                    {
                        int diffDays = (interpolatedList[j].Date - currentDate).Days;
                        if (diffDays > interpolationWindowDays) break;

                        if (interpolatedList[j].Value != null && interpolatedList[j].Value != 0)
                        {
                             if (diffDays < daysToClosestAfter) {
                                closestAfterValue = interpolatedList[j].Value;
                                daysToClosestAfter = diffDays; // 실제 경과일 기록
                                break; // 가장 가까운 하나를 찾으면 중단
                            }
                        }
                    }

                    // 3. 보간 적용
                    if (closestBeforeValue != null && closestAfterValue != null)
                    {
                        // 두 값 사이의 단순 평균으로 보간 (JS_DAMRSRT 방식)
                        // 더 정교한 선형 보간은 (y2-y1)/(x2-x1) * (x-x1) + y1 공식을 사용해야 함.
                        // JS_DAMRSRT는 단순 평균을 사용하므로, 그 방식을 따름.
                        interpolatedList[i].Value = (closestBeforeValue + closestAfterValue) / 2.0;
                        interpolatedList[i].Interpolated = true;

                        logger?.Info($"보간 적용: 날짜={currentDate:yyyy-MM-dd}, 보간값={interpolatedList[i].Value:F2} (이전값:{closestBeforeValue:F2} [{daysToClosestBefore}일 전], 이후값:{closestAfterValue:F2} [{daysToClosestAfter}일 후])", context);
                    }
                    else if (closestBeforeValue != null) // 앞쪽 값만 있는 경우
                    {
                        // JS_DAMRSRT는 이 경우 보간 안 함. 필요시 정책 추가 (예: 앞쪽 값 사용)
                        logger?.Debug($"단방향 보간(이전 값만 유효) 건너뜀: 날짜={currentDate:yyyy-MM-dd}, 이전값={closestBeforeValue:F2} [{daysToClosestBefore}일 전]", context);

                    }
                    else if (closestAfterValue != null) // 뒤쪽 값만 있는 경우
                    {
                        // JS_DAMRSRT는 이 경우 보간 안 함. 필요시 정책 추가 (예: 뒤쪽 값 사용)
                         logger?.Debug($"단방향 보간(이후 값만 유효) 건너뜀: 날짜={currentDate:yyyy-MM-dd}, 이후값={closestAfterValue:F2} [{daysToClosestAfter}일 후]", context);
                    }
                    else // 양쪽 모두 유효한 값이 없는 경우
                    {
                        logger?.Warn($"보간 실패 (양쪽 유효값 없음): 날짜={currentDate:yyyy-MM-dd}", context);
                    }
                }
            }
            return interpolatedList;
        }
    }
}
