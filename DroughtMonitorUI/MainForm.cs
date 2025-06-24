// DroughtMonitorUI/MainForm.cs
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using DroughtCore.Configuration;
using DroughtCore.Logging;

namespace DroughtMonitorUI
{
    public partial class MainForm : Form
    {
        private TabControl mainTabControl;
        private TabPage collectorTabPage;
        private TabPage processorTabPage;
        private TabPage rRunnerTabPage;
        private TabPage logTabPage;
        private TabPage configTabPage;

        private Button btnRunDataCollector;
        private DataGridView dgvCollectorStatus;

        private Button btnRunDataProcessor;
        private DataGridView dgvProcessorStatus;

        private Button btnRunRScript;
        private TextBox txtRScriptOutput;

        private ListBox lstLogOutput;
        private ComboBox cmbLogLevelFilter;
        private TextBox txtLogContextFilter;
        private Button btnApplyLogFilter;

        private TextBox txtConfigContent;
        private Button btnReloadConfig;

        private StatusStrip statusStrip;
        private ToolStripStatusLabel lblStatus;

        // GMLogManager는 정적 클래스이므로 멤버로 둘 필요 없음
        private ConfigManager _configManager;

        private string _dataCollectorExePath;
        private string _dataProcessorExePath;
        private string _rRunnerExePath;

        private List<string> _allLogEntries = new List<string>();
        private const int MaxLogEntries = 2000;

        private class ProcessStatusInfo
        {
            public string ProcessName { get; set; }
            public string Status { get; set; }
            public DateTime? LastRunTime { get; set; }
            public DateTime? LastSuccessTime { get; set; }
            public string Message { get; set; }
        }
        private List<ProcessStatusInfo> _collectorStatuses = new List<ProcessStatusInfo>();
        private List<ProcessStatusInfo> _processorStatuses = new List<ProcessStatusInfo>();

        public MainForm()
        {
            InitializeComponent();
            InitializeAppAsync();
        }

        private async void InitializeAppAsync()
        {
            GMLogManager.Configure("log4net.config"); // log4net 설정
            LogMessageToUI(LogLevel.Info, "DroughtMonitorUI 애플리케이션 시작...", "UI.Init");

            try
            {
                _configManager = new ConfigManager();
                DisplayConfig();
                LoadExecutablePaths();
                InitializeProcessStatuses();
            }
            catch (Exception ex)
            {
                LogMessageToUI(LogLevel.Fatal, $"초기화 중 설정 파일 로드 실패: {ex.Message}", "UI.Init", ex);
                GMLogManager.Fatal($"초기화 중 설정 파일 로드 실패: {ex.Message}", ex, "UI.Init");
                MessageBox.Show($"설정 파일 로드에 심각한 오류가 발생했습니다: {ex.Message}\n애플리케이션을 종료합니다.", "초기화 오류", MessageBoxButtons.OK, MessageBoxIcon.Error);
                Application.Exit();
            }
        }

        private void InitializeProcessStatuses()
        {
            var collectorNames = new List<string> {
                "WAMIS 댐 수문", "WAMIS 유량", "EcoWater 저수지",
                "KWeather ASOS", "토양수분 API", "하천유량(대안) API"
            };
            _collectorStatuses.Clear();
            foreach(var name in collectorNames)
            {
                _collectorStatuses.Add(new ProcessStatusInfo { ProcessName = name, Status = "대기" });
            }
            RefreshDataGridView(dgvCollectorStatus, _collectorStatuses);

            var processorNames = new List<string> {
                 "면적 강우", "댐 저수율", "저수지 저수율(AR)", "유량(FR)", "농업용수(AgAG)"
            };
            _processorStatuses.Clear();
            foreach(var name in processorNames)
            {
                 _processorStatuses.Add(new ProcessStatusInfo { ProcessName = name, Status = "대기" });
            }
            RefreshDataGridView(dgvProcessorStatus, _processorStatuses);
        }

        private void LoadExecutablePaths()
        {
            string baseDir = AppDomain.CurrentDomain.BaseDirectory;
            var exeSettings = _configManager.Settings.ExecutablePaths;

            _dataCollectorExePath = GetFullPath(baseDir, exeSettings?.DataCollectorExePath);
            _dataProcessorExePath = GetFullPath(baseDir, exeSettings?.DataProcessorExePath);
            _rRunnerExePath = GetFullPath(baseDir, exeSettings?.RRunnerExePath);

            LogMessageToUI(LogLevel.Debug, $"Collector Exe Path: {_dataCollectorExePath}", "UI.Init");
            GMLogManager.Debug($"Collector Exe Path: {_dataCollectorExePath}", "UI.Init");
            LogMessageToUI(LogLevel.Debug, $"Processor Exe Path: {_dataProcessorExePath}", "UI.Init");
            GMLogManager.Debug($"Processor Exe Path: {_dataProcessorExePath}", "UI.Init");
            LogMessageToUI(LogLevel.Debug, $"R Runner Exe Path: {_rRunnerExePath}", "UI.Init");
            GMLogManager.Debug($"R Runner Exe Path: {_rRunnerExePath}", "UI.Init");

            if (string.IsNullOrEmpty(_dataCollectorExePath) || !File.Exists(_dataCollectorExePath)) LogMessageToUI(LogLevel.Warn, $"DroughtDataCollector.exe 를 찾을 수 없거나 경로가 설정되지 않았습니다: '{_dataCollectorExePath}'", "UI.Init");
            if (string.IsNullOrEmpty(_dataProcessorExePath) || !File.Exists(_dataProcessorExePath)) LogMessageToUI(LogLevel.Warn, $"DroughtDataProcessor.exe 를 찾을 수 없거나 경로가 설정되지 않았습니다: '{_dataProcessorExePath}'", "UI.Init");
            if (string.IsNullOrEmpty(_rRunnerExePath) || !File.Exists(_rRunnerExePath)) LogMessageToUI(LogLevel.Warn, $"DroughtRRunner.exe 를 찾을 수 없거나 경로가 설정되지 않았습니다: '{_rRunnerExePath}'", "UI.Init");
        }

        private string GetFullPath(string baseDir, string configuredPath)
        {
            if (string.IsNullOrEmpty(configuredPath)) return null;
            if (Path.IsPathRooted(configuredPath)) return configuredPath;
            try
            {
                return Path.GetFullPath(Path.Combine(baseDir, configuredPath));
            }
            catch (Exception ex)
            {
                LogMessageToUI(LogLevel.Error, $"실행 파일 경로 구성 오류: Base='{baseDir}', Configured='{configuredPath}'", ex, "UI.Init");
                GMLogManager.Error($"실행 파일 경로 구성 오류: Base='{baseDir}', Configured='{configuredPath}'", ex, "UI.Init");
                return null;
            }
        }

        private void InitializeComponent()
        {
            this.mainTabControl = new System.Windows.Forms.TabControl();
            this.collectorTabPage = new System.Windows.Forms.TabPage();
            this.processorTabPage = new System.Windows.Forms.TabPage();
            this.rRunnerTabPage = new System.Windows.Forms.TabPage();
            this.logTabPage = new System.Windows.Forms.TabPage();
            this.configTabPage = new System.Windows.Forms.TabPage();

            this.btnRunDataCollector = new System.Windows.Forms.Button();
            this.dgvCollectorStatus = new System.Windows.Forms.DataGridView();
            this.btnRunDataProcessor = new System.Windows.Forms.Button();
            this.dgvProcessorStatus = new System.Windows.Forms.DataGridView();
            this.btnRunRScript = new System.Windows.Forms.Button();
            this.txtRScriptOutput = new System.Windows.Forms.TextBox();

            this.lstLogOutput = new System.Windows.Forms.ListBox();
            this.cmbLogLevelFilter = new System.Windows.Forms.ComboBox();
            this.txtLogContextFilter = new System.Windows.Forms.TextBox();
            this.btnApplyLogFilter = new System.Windows.Forms.Button();

            this.txtConfigContent = new System.Windows.Forms.TextBox();
            this.btnReloadConfig = new System.Windows.Forms.Button();

            this.statusStrip = new System.Windows.Forms.StatusStrip();
            this.lblStatus = new System.Windows.Forms.ToolStripStatusLabel();
            this.SuspendLayout();

            // TabControl
            this.mainTabControl.Dock = System.Windows.Forms.DockStyle.Fill;
            this.mainTabControl.Controls.Add(this.collectorTabPage);
            this.mainTabControl.Controls.Add(this.processorTabPage);
            this.mainTabControl.Controls.Add(this.rRunnerTabPage);
            this.mainTabControl.Controls.Add(this.logTabPage);
            this.mainTabControl.Controls.Add(this.configTabPage);
            this.mainTabControl.Name = "mainTabControl";
            this.mainTabControl.SelectedIndex = 0;
            this.mainTabControl.Size = new System.Drawing.Size(784, 538);
            this.mainTabControl.TabIndex = 0;

            // collectorTabPage
            this.collectorTabPage.Controls.Add(this.dgvCollectorStatus);
            this.collectorTabPage.Controls.Add(this.btnRunDataCollector);
            this.collectorTabPage.Location = new System.Drawing.Point(4, 22); // 기본 탭 높이 고려
            this.collectorTabPage.Name = "collectorTabPage";
            this.collectorTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.collectorTabPage.Size = new System.Drawing.Size(776, 512); // 탭 내부 크기 조정
            this.collectorTabPage.TabIndex = 0;
            this.collectorTabPage.Text = "데이터 수집";
            this.collectorTabPage.UseVisualStyleBackColor = true;

            // btnRunDataCollector
            this.btnRunDataCollector.Location = new System.Drawing.Point(8, 6);
            this.btnRunDataCollector.Name = "btnRunDataCollector";
            this.btnRunDataCollector.Size = new System.Drawing.Size(140, 23);
            this.btnRunDataCollector.TabIndex = 0;
            this.btnRunDataCollector.Text = "전체 수집 실행";
            this.btnRunDataCollector.UseVisualStyleBackColor = true;
            this.btnRunDataCollector.Click += new System.EventHandler(this.BtnRunDataCollector_Click);

            // dgvCollectorStatus
            this.dgvCollectorStatus.AllowUserToAddRows = false;
            this.dgvCollectorStatus.AllowUserToDeleteRows = false;
            this.dgvCollectorStatus.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
            | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.dgvCollectorStatus.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgvCollectorStatus.Location = new System.Drawing.Point(8, 35);
            this.dgvCollectorStatus.Name = "dgvCollectorStatus";
            this.dgvCollectorStatus.ReadOnly = true;
            this.dgvCollectorStatus.RowHeadersVisible = false;
            this.dgvCollectorStatus.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.dgvCollectorStatus.Size = new System.Drawing.Size(760, 470); // 크기 조정
            this.dgvCollectorStatus.TabIndex = 1;
            this.dgvCollectorStatus.ColumnCount = 5;
            this.dgvCollectorStatus.Columns[0].Name = "API 이름";
            this.dgvCollectorStatus.Columns[1].Name = "상태";
            this.dgvCollectorStatus.Columns[2].Name = "마지막 실행";
            this.dgvCollectorStatus.Columns[3].Name = "마지막 성공";
            this.dgvCollectorStatus.Columns[4].Name = "메시지";
            this.dgvCollectorStatus.Columns["메시지"].AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill;

            // processorTabPage
            this.processorTabPage.Controls.Add(this.dgvProcessorStatus);
            this.processorTabPage.Controls.Add(this.btnRunDataProcessor);
            this.processorTabPage.Location = new System.Drawing.Point(4, 22);
            this.processorTabPage.Name = "processorTabPage";
            this.processorTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.processorTabPage.Size = new System.Drawing.Size(776, 512);
            this.processorTabPage.TabIndex = 1;
            this.processorTabPage.Text = "데이터 처리";
            this.processorTabPage.UseVisualStyleBackColor = true;

            // btnRunDataProcessor
            this.btnRunDataProcessor.Location = new System.Drawing.Point(8, 6);
            this.btnRunDataProcessor.Name = "btnRunDataProcessor";
            this.btnRunDataProcessor.Size = new System.Drawing.Size(140, 23);
            this.btnRunDataProcessor.TabIndex = 0;
            this.btnRunDataProcessor.Text = "전체 처리 실행";
            this.btnRunDataProcessor.UseVisualStyleBackColor = true;
            this.btnRunDataProcessor.Click += new System.EventHandler(this.BtnRunDataProcessor_Click);

            // dgvProcessorStatus
             this.dgvProcessorStatus.AllowUserToAddRows = false;
            this.dgvProcessorStatus.AllowUserToDeleteRows = false;
            this.dgvProcessorStatus.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
            | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.dgvProcessorStatus.ColumnHeadersHeightSizeMode = System.Windows.Forms.DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            this.dgvProcessorStatus.Location = new System.Drawing.Point(8, 35);
            this.dgvProcessorStatus.Name = "dgvProcessorStatus";
            this.dgvProcessorStatus.ReadOnly = true;
            this.dgvProcessorStatus.RowHeadersVisible = false;
            this.dgvProcessorStatus.SelectionMode = System.Windows.Forms.DataGridViewSelectionMode.FullRowSelect;
            this.dgvProcessorStatus.Size = new System.Drawing.Size(760, 470);
            this.dgvProcessorStatus.TabIndex = 1;
            this.dgvProcessorStatus.ColumnCount = 5;
            this.dgvProcessorStatus.Columns[0].Name = "데이터 종류";
            this.dgvProcessorStatus.Columns[1].Name = "상태";
            this.dgvProcessorStatus.Columns[2].Name = "마지막 실행";
            this.dgvProcessorStatus.Columns[3].Name = "마지막 성공";
            this.dgvProcessorStatus.Columns[4].Name = "메시지";
            this.dgvProcessorStatus.Columns["메시지"].AutoSizeMode = DataGridViewAutoSizeColumnMode.Fill;

            // rRunnerTabPage
            this.rRunnerTabPage.Controls.Add(this.txtRScriptOutput);
            this.rRunnerTabPage.Controls.Add(this.btnRunRScript);
            this.rRunnerTabPage.Location = new System.Drawing.Point(4, 22);
            this.rRunnerTabPage.Name = "rRunnerTabPage";
            this.rRunnerTabPage.Size = new System.Drawing.Size(776, 512);
            this.rRunnerTabPage.TabIndex = 2;
            this.rRunnerTabPage.Text = "R 스크립트";
            this.rRunnerTabPage.UseVisualStyleBackColor = true;

            // btnRunRScript
            this.btnRunRScript.Location = new System.Drawing.Point(8, 6);
            this.btnRunRScript.Name = "btnRunRScript";
            this.btnRunRScript.Size = new System.Drawing.Size(140, 23);
            this.btnRunRScript.TabIndex = 0;
            this.btnRunRScript.Text = "R 스크립트 실행";
            this.btnRunRScript.UseVisualStyleBackColor = true;
            this.btnRunRScript.Click += new System.EventHandler(this.BtnRunRScript_Click);

            // txtRScriptOutput
            this.txtRScriptOutput.Anchor = ((System.Windows.Forms.AnchorStyles)((((System.Windows.Forms.AnchorStyles.Top | System.Windows.Forms.AnchorStyles.Bottom)
            | System.Windows.Forms.AnchorStyles.Left)
            | System.Windows.Forms.AnchorStyles.Right)));
            this.txtRScriptOutput.Location = new System.Drawing.Point(8, 35);
            this.txtRScriptOutput.Multiline = true;
            this.txtRScriptOutput.Name = "txtRScriptOutput";
            this.txtRScriptOutput.ReadOnly = true;
            this.txtRScriptOutput.ScrollBars = System.Windows.Forms.ScrollBars.Both;
            this.txtRScriptOutput.Size = new System.Drawing.Size(760, 470);
            this.txtRScriptOutput.TabIndex = 1;

            // logTabPage
            Panel logFilterPanel = new Panel();
            logFilterPanel.Dock = DockStyle.Top;
            logFilterPanel.Height = 28;
            logFilterPanel.Padding = new Padding(3);

            this.cmbLogLevelFilter.Dock = DockStyle.Left;
            this.cmbLogLevelFilter.DropDownStyle = ComboBoxStyle.DropDownList;
            this.cmbLogLevelFilter.Width = 100;
            this.cmbLogLevelFilter.Items.AddRange(Enum.GetNames(typeof(LogLevel)));
            this.cmbLogLevelFilter.SelectedIndex = (int)LogLevel.Debug;

            this.txtLogContextFilter.Dock = DockStyle.Fill;

            this.btnApplyLogFilter.Dock = DockStyle.Right;
            this.btnApplyLogFilter.Text = "필터 적용";
            this.btnApplyLogFilter.Width = 80;
            this.btnApplyLogFilter.Click += new System.EventHandler(this.BtnApplyLogFilter_Click);

            logFilterPanel.Controls.Add(this.txtLogContextFilter);
            logFilterPanel.Controls.Add(this.cmbLogLevelFilter);
            logFilterPanel.Controls.Add(this.btnApplyLogFilter);

            this.lstLogOutput.Dock = System.Windows.Forms.DockStyle.Fill;
            this.lstLogOutput.FormattingEnabled = true;
            this.lstLogOutput.ItemHeight = 15;
            this.lstLogOutput.Name = "lstLogOutput";

            this.logTabPage.Controls.Add(this.lstLogOutput);
            this.logTabPage.Controls.Add(logFilterPanel);
            this.logTabPage.Location = new System.Drawing.Point(4, 22);
            this.logTabPage.Name = "logTabPage";
            this.logTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.logTabPage.Size = new System.Drawing.Size(776, 512);
            this.logTabPage.TabIndex = 3;
            this.logTabPage.Text = "로그";
            this.logTabPage.UseVisualStyleBackColor = true;

            // configTabPage
            this.btnReloadConfig.Dock = DockStyle.Top;
            this.btnReloadConfig.Height = 23;
            this.btnReloadConfig.Text = "설정 새로고침";
            this.btnReloadConfig.Click += (s,e) => {
                try {
                    _configManager = new ConfigManager();
                    DisplayConfig();
                    LoadExecutablePaths();
                    LogMessageToUI(LogLevel.Info, "설정 파일 새로고침 완료.", "UI.Config");
                    GMLogManager.Info("설정 파일 새로고침 완료.", "UI.Config");
                } catch (Exception ex) {
                    LogMessageToUI(LogLevel.Error, "설정 파일 새로고침 중 오류 발생.", ex, "UI.Config");
                    GMLogManager.Error("설정 파일 새로고침 중 오류 발생.", ex, "UI.Config");
                }
            };
            this.txtConfigContent.Dock = System.Windows.Forms.DockStyle.Fill;
            this.txtConfigContent.Multiline = true;
            this.txtConfigContent.Name = "txtConfigContent";
            this.txtConfigContent.ReadOnly = true;
            this.txtConfigContent.ScrollBars = System.Windows.Forms.ScrollBars.Both;

            this.configTabPage.Controls.Add(this.txtConfigContent);
            this.configTabPage.Controls.Add(this.btnReloadConfig);
            this.configTabPage.Location = new System.Drawing.Point(4, 22);
            this.configTabPage.Name = "configTabPage";
            this.configTabPage.Padding = new System.Windows.Forms.Padding(3);
            this.configTabPage.Size = new System.Drawing.Size(776, 512);
            this.configTabPage.TabIndex = 4;
            this.configTabPage.Text = "설정";
            this.configTabPage.UseVisualStyleBackColor = true;

            // statusStrip
            this.statusStrip.Items.AddRange(new System.Windows.Forms.ToolStripItem[] { this.lblStatus });
            this.statusStrip.Location = new System.Drawing.Point(0, 539);
            this.statusStrip.Name = "statusStrip";
            this.statusStrip.Size = new System.Drawing.Size(784, 22);
            this.statusStrip.TabIndex = 1;
            this.lblStatus.Name = "lblStatus";
            this.lblStatus.Size = new System.Drawing.Size(55, 17); // 기본값, 실제 텍스트에 따라 변경됨
            this.lblStatus.Text = "준비 완료";

            // MainForm
            this.AutoScaleDimensions = new System.Drawing.SizeF(7F, 12F); // 일반적인 한국어 Windows Font 기준
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(784, 561);
            this.Controls.Add(this.mainTabControl);
            this.Controls.Add(this.statusStrip);
            this.Name = "MainForm";
            this.Text = "통합 가뭄 정보 시스템 관리 도구";
            this.mainTabControl.ResumeLayout(false);
            this.collectorTabPage.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.dgvCollectorStatus)).EndInit();
            this.processorTabPage.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)(this.dgvProcessorStatus)).EndInit();
            this.rRunnerTabPage.ResumeLayout(false);
            this.rRunnerTabPage.PerformLayout();
            this.logTabPage.ResumeLayout(false);
            this.configTabPage.ResumeLayout(false);
            // this.configTabPage.PerformLayout(); // txtConfigContent가 Fill이라 필요 없을 수 있음
            this.statusStrip.ResumeLayout(false);
            this.statusStrip.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();
        }

        private void DisplayConfig()
        {
            if (_configManager?.Settings != null)
            {
                try
                {
                    txtConfigContent.Text = Newtonsoft.Json.JsonConvert.SerializeObject(_configManager.Settings, Newtonsoft.Json.Formatting.Indented);
                    LogMessageToUI(LogLevel.Info, "설정 정보 UI에 표시됨.", "UI.Config");
                }
                catch (Exception ex)
                {
                    txtConfigContent.Text = "설정 정보를 표시하는 중 오류가 발생했습니다.";
                    LogMessageToUI(LogLevel.Error, "설정 정보 표시 오류", ex, "UI.Config");
                }
            }
        }

        private void LogMessageToUI(LogLevel level, string message, string context = null, Exception exception = null)
        {
            string logEntry = $"[{DateTime.Now:HH:mm:ss.fff}] [{level.ToString().ToUpper()}]";
            if (!string.IsNullOrEmpty(context)) logEntry += $" [{context}]";
            logEntry += $" {message}";
            if (exception != null) logEntry += $"\n    Exception Details: {exception.ToString()}";

            lock (_allLogEntries)
            {
                _allLogEntries.Insert(0, logEntry);
                if (_allLogEntries.Count > MaxLogEntries * 2)
                {
                    _allLogEntries.RemoveRange(MaxLogEntries * 2, _allLogEntries.Count - (MaxLogEntries * 2));
                }
            }

            ApplyLogFiltersToListBox();

            if (level >= LogLevel.Info) {
                UpdateStatusBar(message);
            }
        }

        private void BtnApplyLogFilter_Click(object sender, EventArgs e)
        {
            ApplyLogFiltersToListBox();
        }

        private void ApplyLogFiltersToListBox()
        {
            if (lstLogOutput.InvokeRequired)
            {
                lstLogOutput.Invoke(new Action(ApplyLogFiltersToListBoxInternal));
            }
            else
            {
                ApplyLogFiltersToListBoxInternal();
            }
        }

        private void ApplyLogFiltersToListBoxInternal()
        {
            LogLevel selectedLevel = LogLevel.Debug;
            if (cmbLogLevelFilter.SelectedIndex >=0) {
                 selectedLevel = (LogLevel)cmbLogLevelFilter.SelectedIndex;
            }
            string contextFilter = txtLogContextFilter.Text.ToLowerInvariant();

            List<string> filteredEntries;
            lock (_allLogEntries)
            {
                filteredEntries = _allLogEntries
                    .Where(entry =>
                    {
                        bool levelMatch = false;
                        try {
                            string entryLevelStr = entry.Substring(entry.IndexOf('[', entry.IndexOf('[') + 1) + 1, entry.IndexOf(']', entry.IndexOf(']') + 1) - (entry.IndexOf('[', entry.IndexOf('[') + 1) + 1));
                            LogLevel entryLevel = (LogLevel)Enum.Parse(typeof(LogLevel), entryLevelStr, true);
                            if (entryLevel >= selectedLevel) levelMatch = true;
                        } catch { levelMatch = true; }

                        bool contextMatch = string.IsNullOrWhiteSpace(contextFilter) || entry.ToLowerInvariant().Contains(contextFilter);

                        return levelMatch && contextMatch;
                    })
                    .Take(MaxLogEntries)
                    .ToList();
            }

            lstLogOutput.BeginUpdate();
            lstLogOutput.Items.Clear();
            lstLogOutput.Items.AddRange(filteredEntries.ToArray());
            lstLogOutput.EndUpdate();
            if (lstLogOutput.Items.Count > 0) lstLogOutput.TopIndex = 0;
        }

        private void UpdateStatusBar(string message)
        {
            if (statusStrip.InvokeRequired) {
                statusStrip.Invoke(new Action(() => lblStatus.Text = message));
            } else {
                lblStatus.Text = message;
            }
        }

        private async Task RunProcessAsync(string processKey, string exePath, string arguments = "", DataGridView dgvToUpdate = null, List<ProcessStatusInfo> statusList = null)
        {
            if (!File.Exists(exePath))
            {
                LogMessageToUI(LogLevel.Error, $"실행 파일을 찾을 수 없습니다: {exePath}", $"UI.Process.{processKey}");
                GMLogManager.Error($"실행 파일을 찾을 수 없습니다: {exePath}", $"UI.Process.{processKey}");
                MessageBox.Show($"실행 파일을 찾을 수 없습니다: {exePath}", "오류", MessageBoxButtons.OK, MessageBoxIcon.Error);
                UpdateProcessStatusInGrid(dgvToUpdate, statusList, processKey, "오류 (파일 없음)", DateTime.Now, null, $"경로: {exePath}");
                return;
            }

            UpdateStatusBar($"{processKey} 실행 중...");
            LogMessageToUI(LogLevel.Info, $"{processKey} 실행 시작. 경로: {exePath} {arguments}", $"UI.Process.{processKey}");
            GMLogManager.Info($"{processKey} 실행 시작. 경로: {exePath} {arguments}", $"UI.Process.{processKey}");
            UpdateProcessStatusInGrid(dgvToUpdate, statusList, processKey, "실행 중", DateTime.Now, null, "시작됨...");

            string processStdOut = string.Empty;
            string processStdErr = string.Empty;

            try
            {
                using (Process process = new Process())
                {
                    process.StartInfo.FileName = exePath;
                    process.StartInfo.Arguments = arguments;
                    process.StartInfo.UseShellExecute = false;
                    process.StartInfo.RedirectStandardOutput = true;
                    process.StartInfo.RedirectStandardError = true;
                    process.StartInfo.CreateNoWindow = true;
                    process.StartInfo.StandardOutputEncoding = System.Text.Encoding.UTF8;
                    process.StartInfo.StandardErrorEncoding = System.Text.Encoding.UTF8;

                    System.Text.StringBuilder outputBuilder = new System.Text.StringBuilder();
                    System.Text.StringBuilder errorBuilder = new System.Text.StringBuilder();

                    process.OutputDataReceived += (s, e) => { if (e.Data != null) { outputBuilder.AppendLine(e.Data); LogMessageToUI(LogLevel.Debug, e.Data, $"{processKey}.Output"); GMLogManager.Debug(e.Data, $"{processKey}.Output"); } };
                    process.ErrorDataReceived += (s, e) => { if (e.Data != null) { errorBuilder.AppendLine(e.Data); LogMessageToUI(LogLevel.Error, e.Data, $"{processKey}.Error"); GMLogManager.Error(e.Data, $"{processKey}.Error"); } };

                    process.Start();
                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();

                    await Task.Run(() => process.WaitForExit());

                    processStdOut = outputBuilder.ToString();
                    processStdErr = errorBuilder.ToString();

                    if (process.ExitCode == 0)
                    {
                        LogMessageToUI(LogLevel.Info, $"{processKey} 실행 완료. Exit Code: {process.ExitCode}", $"UI.Process.{processKey}");
                        GMLogManager.Info($"{processKey} 실행 완료. Exit Code: {process.ExitCode}", $"UI.Process.{processKey}");
                        UpdateProcessStatusInGrid(dgvToUpdate, statusList, processKey, "성공", DateTime.Now, DateTime.Now, "정상 종료");
                    }
                    else
                    {
                        LogMessageToUI(LogLevel.Error, $"{processKey} 실행 오류. Exit Code: {process.ExitCode}. Stderr: {processStdErr}", $"UI.Process.{processKey}");
                        GMLogManager.Error($"{processKey} 실행 오류. Exit Code: {process.ExitCode}. Stderr: {processStdErr}", $"UI.Process.{processKey}");
                        UpdateProcessStatusInGrid(dgvToUpdate, statusList, processKey, "실패", DateTime.Now, null, $"오류 (Code: {process.ExitCode})");
                    }
                }
            }
            catch (Exception ex)
            {
                LogMessageToUI(LogLevel.Fatal, $"{processKey} 실행 중 예외 발생", ex, $"UI.Process.{processKey}");
                GMLogManager.Fatal($"{processKey} 실행 중 예외 발생", ex, $"UI.Process.{processKey}");
                MessageBox.Show($"{processKey} 실행 중 예외: {ex.Message}", "실행 오류", MessageBoxButtons.OK, MessageBoxIcon.Error);
                UpdateProcessStatusInGrid(dgvToUpdate, statusList, processKey, "실패 (예외)", DateTime.Now, null, ex.Message);
            }
            finally
            {
                 UpdateStatusBar($"{processKey} 완료.");
                 if (processKey == "R 스크립트") {
                    if (txtRScriptOutput.InvokeRequired) {
                        txtRScriptOutput.Invoke(new Action(() => txtRScriptOutput.Text = $"Standard Output:\r\n{processStdOut}\r\nStandard Error:\r\n{processStdErr}"));
                    } else {
                        txtRScriptOutput.Text = $"Standard Output:\r\n{processStdOut}\r\nStandard Error:\r\n{processStdErr}";
                    }
                 }
            }
        }

        private void UpdateProcessStatusInGrid(DataGridView dgv, List<ProcessStatusInfo> statusList, string processName, string status, DateTime? lastRunTime, DateTime? lastSuccessTime, string message)
        {
            if (dgv == null || statusList == null) return;

            var statusInfo = statusList.FirstOrDefault(s => s.ProcessName == processName);
            if (statusInfo == null)
            {
                 LogMessageToUI(LogLevel.Warn, $"ProcessStatusInfo에서 '{processName}'을 찾을 수 없습니다.", "UI.UpdateStatus");
                 GMLogManager.Warn($"ProcessStatusInfo에서 '{processName}'을 찾을 수 없습니다.", "UI.UpdateStatus");
                return;
            }

            statusInfo.Status = status;
            if (lastRunTime.HasValue) statusInfo.LastRunTime = lastRunTime;
            if (lastSuccessTime.HasValue) statusInfo.LastSuccessTime = lastSuccessTime;
            statusInfo.Message = message;

            RefreshDataGridView(dgv, statusList); // DataGridView 갱신 호출
        }

        private void RefreshDataGridView(DataGridView dgv, List<ProcessStatusInfo> statusList)
        {
            if (dgv.InvokeRequired)
            {
                dgv.Invoke(new Action(() => RefreshDataGridViewInternal(dgv, statusList) ));
            }
            else
            {
                RefreshDataGridViewInternal(dgv, statusList);
            }
        }
        private void RefreshDataGridViewInternal(DataGridView dgv, List<ProcessStatusInfo> statusList)
        {
            dgv.Rows.Clear();
            if (statusList != null) {
                foreach (var status in statusList)
                {
                    dgv.Rows.Add(status.ProcessName, status.Status ?? "대기",
                                 status.LastRunTime?.ToString("yyyy-MM-dd HH:mm:ss"),
                                 status.LastSuccessTime?.ToString("yyyy-MM-dd HH:mm:ss"),
                                 status.Message);
                }
            }
        }


        private async void BtnRunDataCollector_Click(object sender, EventArgs e)
        {
            await RunProcessAsync("데이터 수집기 전체", _dataCollectorExePath, dgvToUpdate: dgvCollectorStatus, statusList: _collectorStatuses);
        }

        private async void BtnRunDataProcessor_Click(object sender, EventArgs e)
        {
            await RunProcessAsync("데이터 처리기 전체", _dataProcessorExePath, dgvToUpdate: dgvProcessorStatus, statusList: _processorStatuses);
        }

        private async void BtnRunRScript_Click(object sender, EventArgs e)
        {
            await RunProcessAsync("R 스크립트", _rRunnerExePath);
        }
    }
}
