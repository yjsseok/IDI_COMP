// DroughtCore/DataAccess/DbService.cs
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DroughtCore.Logging; // GMLogManager 사용
using DroughtCore.Models;   // 예시 모델 사용
using System.Globalization; // CultureInfo 추가

namespace DroughtCore.DataAccess
{
    public class DbService
    {
        private readonly string _connectionString;
        // GMLogManager는 정적 클래스이므로 멤버로 둘 필요 없음

        public DbService(string connectionString) // 로거 인자 제거
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
            // GMLogManager.Configure(); // Configure는 Program.cs 등 앱 시작점에서 한 번만 호출
        }

        private async Task<NpgsqlConnection> GetOpenConnectionAsync()
        {
            var conn = new NpgsqlConnection(_connectionString);
            try
            {
                await conn.OpenAsync();
                // GMLogManager.Debug("PostgreSQL connection opened.", "DbService");
                return conn;
            }
            catch (Exception ex)
            {
                GMLogManager.Error("PostgreSQL 연결 열기 실패.", ex, "DbService");
                throw;
            }
        }

        public async Task<int> ExecuteNonQueryAsync(string query, params NpgsqlParameter[] parameters)
        {
            try
            {
                using (var conn = await GetOpenConnectionAsync())
                using (var cmd = new NpgsqlCommand(query, conn))
                {
                    if (parameters != null)
                    {
                        cmd.Parameters.AddRange(parameters);
                    }
                    int affectedRows = await cmd.ExecuteNonQueryAsync();
                    GMLogManager.Debug($"ExecuteNonQuery 성공: {affectedRows} 행 영향 받음. Query: {query?.Substring(0, Math.Min(query.Length, 200))}", "DbService"); // 쿼리 길이 제한
                    return affectedRows;
                }
            }
            catch (Exception ex)
            {
                GMLogManager.Error($"ExecuteNonQuery 중 오류 발생. Query: {query?.Substring(0, Math.Min(query.Length, 200))}", ex, "DbService");
                throw;
            }
        }

        public async Task<object> ExecuteScalarAsync(string query, params NpgsqlParameter[] parameters)
        {
            try
            {
                using (var conn = await GetOpenConnectionAsync())
                using (var cmd = new NpgsqlCommand(query, conn))
                {
                    if (parameters != null)
                    {
                        cmd.Parameters.AddRange(parameters);
                    }
                    object result = await cmd.ExecuteScalarAsync();
                    GMLogManager.Debug($"ExecuteScalar 성공. Result: {result}. Query: {query?.Substring(0, Math.Min(query.Length, 200))}", "DbService");
                    return result;
                }
            }
            catch (Exception ex)
            {
                GMLogManager.Error($"ExecuteScalar 중 오류 발생. Query: {query?.Substring(0, Math.Min(query.Length, 200))}", ex, "DbService");
                throw;
            }
        }

        public async Task<DataTable> GetDataTableAsync(string query, params NpgsqlParameter[] parameters)
        {
            var dataTable = new DataTable();
            try
            {
                using (var conn = await GetOpenConnectionAsync())
                using (var cmd = new NpgsqlCommand(query, conn))
                {
                    if (parameters != null)
                    {
                        cmd.Parameters.AddRange(parameters);
                    }
                    using (var adapter = new NpgsqlDataAdapter(cmd))
                    {
                        await Task.Run(() => adapter.Fill(dataTable));
                    }
                    GMLogManager.Debug($"GetDataTable 성공: {dataTable.Rows.Count} 행 로드됨. Query: {query?.Substring(0, Math.Min(query.Length, 200))}", "DbService");
                }
            }
            catch (Exception ex)
            {
                GMLogManager.Error($"GetDataTable 중 오류 발생. Query: {query?.Substring(0, Math.Min(query.Length, 200))}", ex, "DbService");
                throw;
            }
            return dataTable;
        }

        public async Task BulkCopyFromCsvLinesAsync(string tableName, string sggCode, IEnumerable<string> csvLines, List<Tuple<string, NpgsqlDbType>> columnMapping)
        {
            if (columnMapping == null || !columnMapping.Any())
            {
                GMLogManager.Error("BulkCopyFromCsvLinesAsync: columnMapping이 비어있습니다.", "DbService");
                throw new ArgumentException("columnMapping은 null이거나 비어 있을 수 없습니다.");
            }

            string columns = string.Join(", ", columnMapping.Select(c => $"\"{c.Item1}\""));
            string copyCommand = $"COPY {tableName} ({columns}) FROM STDIN (FORMAT BINARY)";

            GMLogManager.Info($"BulkCopy 시작: Table='{tableName}', SggCode='{sggCode}', Command='{copyCommand}'", "DbService");

            try
            {
                using (var conn = await GetOpenConnectionAsync())
                using (var tx = await conn.BeginTransactionAsync())
                {
                    if (columnMapping.Any(c => c.Item1.Equals("sgg_cd", StringComparison.OrdinalIgnoreCase)))
                    {
                        var deleteCmdText = $"DELETE FROM {tableName} WHERE sgg_cd = @sgg_cd";
                        using (var deleteCmd = new NpgsqlCommand(deleteCmdText, conn, tx))
                        {
                            deleteCmd.Parameters.AddWithValue("@sgg_cd", sggCode);
                            int deletedRows = await deleteCmd.ExecuteNonQueryAsync();
                            GMLogManager.Info($"기존 데이터 삭제: {deletedRows} 행 (Table: {tableName}, SggCode: {sggCode})", "DbService");
                        }
                    } else {
                         GMLogManager.Warn($"BulkCopy: {tableName} 테이블에 sgg_cd 컬럼 매핑이 없어 삭제를 건너<0xEB><0x9B><0x84>니다. 중복 데이터가 발생할 수 있습니다.", "DbService");
                    }

                    using (var importer = await conn.BeginBinaryImportAsync(copyCommand))
                    {
                        foreach (var line in csvLines)
                        {
                            var parts = line.Split(',');
                            bool sggCdColumnExists = columnMapping.Any(c => c.Item1.Equals("sgg_cd", StringComparison.OrdinalIgnoreCase));
                            int expectedParts = sggCdColumnExists ? columnMapping.Count -1 : columnMapping.Count;

                            if (parts.Length < expectedParts) {
                                GMLogManager.Warn($"데이터 라인 파싱 오류 (컬럼 수 부족): '{line}'. 예상 컬럼 수(sgg_cd 제외 시): {expectedParts}, 실제: {parts.Length}. 건너<0xEB><0x9B><0x84>니다.", "DbService");
                                continue;
                            }

                            await importer.StartRowAsync();
                            int partIndex = 0;
                            foreach (var colMap in columnMapping)
                            {
                                object valueToImport;
                                string currentValue;

                                if (colMap.Item1.Equals("sgg_cd", StringComparison.OrdinalIgnoreCase))
                                {
                                    currentValue = sggCode;
                                }
                                else
                                {
                                    if (partIndex >= parts.Length) {
                                        GMLogManager.Error($"BulkCopy 중 컬럼 인덱스 오류: line='{line}', col='{colMap.Item1}', partIndex='{partIndex}', parts.Length='{parts.Length}'", "DbService");
                                        await importer.WriteNullAsync();
                                        partIndex++;
                                        continue;
                                    }
                                    currentValue = parts[partIndex++];
                                }

                                // "-9999"나 빈 값, 또는 특정 컬럼("data", "flow")의 "0"을 null로 처리 (DB에 따라 다를 수 있음)
                                bool treatAsNull = string.IsNullOrWhiteSpace(currentValue) || currentValue == "-9999";
                                if (!treatAsNull && (colMap.Item1.Equals("data", StringComparison.OrdinalIgnoreCase) || colMap.Item1.Equals("flow", StringComparison.OrdinalIgnoreCase) || colMap.Item1.Equals("rsrt", StringComparison.OrdinalIgnoreCase)))
                                {
                                    if (currentValue == "0" || currentValue == "0.0" || currentValue == "0.00") // 0도 결측치처럼 처리해야 하는 경우
                                    {
                                       // JS_DAMRSRT에서는 0도 저장 안하는 경우가 있었음. 정책에 따라 이 부분 조정.
                                       // 여기서는 0을 유효값으로 보고 그대로 파싱 시도. 만약 0을 null로 해야한다면 treatAsNull = true;
                                    }
                                }
                                if (colMap.Item1.Equals("jd", StringComparison.OrdinalIgnoreCase) && currentValue == "0") // jd는 0일 수 있음
                                {
                                   // 0을 유효값으로 처리
                                } else if (string.IsNullOrWhiteSpace(currentValue) || currentValue.Equals("-9999", StringComparison.OrdinalIgnoreCase) || (currentValue == "0" && colMap.Item1 != "jd"))
                                { // jd가 아닌데 0이면 결측으로 볼 수 도 있음 (JS_DAMRSRT 로직 참고)
                                    // data, flow, rsrt 컬럼의 0 값을 어떻게 처리할지 정책 필요.
                                    // 여기서는 -9999나 공백만 null로 처리. 0은 유효값으로 파싱 시도.
                                    if(string.IsNullOrWhiteSpace(currentValue) || currentValue == "-9999") treatAsNull = true;
                                }


                                if (treatAsNull)
                                {
                                    await importer.WriteNullAsync();
                                    continue;
                                }

                                try
                                {
                                    switch (colMap.Item2)
                                    {
                                        case NpgsqlDbType.Integer:
                                            valueToImport = int.Parse(currentValue, CultureInfo.InvariantCulture);
                                            break;
                                        case NpgsqlDbType.Double:
                                            valueToImport = double.Parse(currentValue, CultureInfo.InvariantCulture);
                                            break;
                                        case NpgsqlDbType.Varchar:
                                        case NpgsqlDbType.Text:
                                            valueToImport = currentValue;
                                            break;
                                        case NpgsqlDbType.Date: // CSV에 YYYYMMDD 형식으로 있다면
                                            valueToImport = DateTime.ParseExact(currentValue, "yyyyMMdd", CultureInfo.InvariantCulture);
                                            break;
                                        default:
                                            GMLogManager.Warn($"지원되지 않는 NpgsqlDbType: {colMap.Item2} for column {colMap.Item1}", "DbService");
                                            await importer.WriteNullAsync();
                                            continue;
                                    }
                                    await importer.WriteAsync(valueToImport, colMap.Item2);
                                }
                                catch (Exception parseEx)
                                {
                                    GMLogManager.Error($"값 파싱/쓰기 오류: Column='{colMap.Item1}', Value='{currentValue}', Type='{colMap.Item2}'. Line: '{line}'", parseEx, "DbService");
                                    await importer.WriteNullAsync();
                                }
                            }
                        }
                        await importer.CompleteAsync();
                    }
                    await tx.CommitAsync();
                    GMLogManager.Info($"BulkCopy 성공: Table='{tableName}', SggCode='{sggCode}', {csvLines.Count()} 라인 처리 시도", "DbService");
                }
            }
            catch (Exception ex)
            {
                GMLogManager.Error($"BulkCopy 중 오류 발생: Table='{tableName}', SggCode='{sggCode}'", ex, "DbService");
                throw;
            }
        }
    }
}
