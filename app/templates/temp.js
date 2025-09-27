$(document).ready(function() {
    console.log('页面加载完成，开始初始化多选筛选器');

    // 初始化所有多选下拉框
    initMultiSelectDropdowns();
    updateColumnVisibility();

    // 绑定显示列的切换事件
    $('#toggleQty, #toggleAmt, #toggleAvg').on('change', updateColumnVisibility);

    // 应用筛选按钮点击事件
    $('#applyFilter').click(function() {
        fetchAnalysisData();
        fetchTransactionTimeData();
    });

    // 页面加载时自动应用一次筛选
    fetchAnalysisData();
    fetchTransactionTimeData();

    // 重置按钮点击事件
    $('button[type="reset"]').click(function() {
        // 重置所有筛选器为全选状态
        $('.multi-select-dropdown').each(function() {
            $(this).find('.option-checkbox').prop('checked', true);
            $(this).find('.select-all').prop('checked', true);
            updateMultiSelectDisplay($(this));
        });
        
        // 清空时间选择器
        $('#production_start_month, #production_end_month, #transaction_start_date, #transaction_end_date').val('');
        
        // 触发一次筛选更新
        updateDynamicFilters();
    });

    function initMultiSelectDropdowns() {
        $('.multi-select-dropdown').each(function() {
            const dropdown = $(this);
            
            // 防止下拉菜单在点击内部时关闭
            dropdown.find('.dropdown-menu').on('click', function(e) {
                e.stopPropagation();
            });

            // 搜索功能
            dropdown.find('.search-input').on('input', function() {
                const searchTerm = $(this).val().toLowerCase();
                dropdown.find('.option-item').each(function() {
                    const labelText = $(this).find('label').text().toLowerCase();
                    $(this).toggle(labelText.includes(searchTerm));
                });
            });

            // 全选功能
            dropdown.find('.select-all').on('change', function() {
                const isChecked = $(this).is(':checked');
                dropdown.find('.option-checkbox:visible').prop('checked', isChecked);
                updateMultiSelectDisplay(dropdown);
                updateDynamicFilters();
            });

            // 单个选项点击事件
            dropdown.on('change', '.option-checkbox', function() {
                updateMultiSelectDisplay(dropdown);
                updateDynamicFilters();
            });
            
            // 初始化显示
            updateMultiSelectDisplay(dropdown);
        });
    }

    function updateMultiSelectDisplay(dropdown) {
        const totalOptions = dropdown.find('.option-checkbox').length;
        const checkedOptions = dropdown.find('.option-checkbox:checked').length;
        const selectedTextSpan = dropdown.find('.selected-text');
        
        if (checkedOptions === 0) {
            selectedTextSpan.text('未选择');
            dropdown.find('.select-all').prop('checked', false).prop('indeterminate', false);
        } else if (checkedOptions === totalOptions) {
            selectedTextSpan.text('--所有--');
            dropdown.find('.select-all').prop('checked', true).prop('indeterminate', false);
        } else {
            selectedTextSpan.text(`已选 ${checkedOptions} 项`);
            dropdown.find('.select-all').prop('indeterminate', true);
        }
    }
    
    // 核心函数：当任何筛选条件变化时，调用此函数更新所有筛选器的可选项
    function updateDynamicFilters() {
        const filters = collectFilters();
        
        console.log("Updating dynamic filters with:", filters);

        $.ajax({
            url: '{{ url_for("dashboard.get_dynamic_filter_options") }}',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify(filters),
            success: function(response) {
                console.log("Received dynamic options:", response);
                // 重新渲染所有下拉框的选项
                $('.multi-select-dropdown').each(function() {
                    const dropdown = $(this);
                    const filterKey = dropdown.data('filter-key');
                    const optionsList = dropdown.find('.options-list');
                    const newOptions = response.options[filterKey] || [];
                    
                    optionsList.empty();
                    
                    // 特殊处理布尔值
                    if (filterKey === 'is_uhv_support' || filterKey === 'has_subsidy') {
                        if (newOptions.some(o => o[0] === true)) {
                             optionsList.append(createOptionItem('是', '1'));
                        }
                        if (newOptions.some(o => o[0] === false)) {
                             optionsList.append(createOptionItem('否', '0'));
                        }
                    } else {
                        newOptions.forEach(function(option) {
                           optionsList.append(createOptionItem(option, option));
                        });
                    }
                    
                    // 保持当前选中状态
                    const currentValues = filters[filterKey] || [];
                    optionsList.find('.option-checkbox').each(function() {
                        if(currentValues.includes($(this).val())) {
                            $(this).prop('checked', true);
                        } else {
                            $(this).prop('checked', false);
                        }
                    });

                    updateMultiSelectDisplay(dropdown);
                });

                // 更新项目列表
                const projectsList = $('#projectsList');
                projectsList.empty();
                if (response.projects && response.projects.length > 0) {
                     response.projects.forEach(function(project) {
                        const projectItem = `
                            <div class="dropdown-item-text project-item">
                                <div class="form-check">
                                    <input class="form-check-input project-checkbox" type="checkbox" value="${project.id}" id="project_${project.id}">
                                    <label class="form-check-label" for="project_${project.id}">${project.project_name}</label>
                                </div>
                            </div>`;
                        projectsList.append(projectItem);
                    });
                } else {
                    projectsList.append('<div class="dropdown-item-text text-muted">没有符合条件的项目</div>');
                }
                // 初始化项目列表（默认全选）
                initProjectSelector();

            },
            error: function(error) {
                console.error("Failed to update dynamic filters:", error);
            }
        });
    }

    function createOptionItem(text, value) {
        return `
            <div class="dropdown-item-text option-item">
              <div class="form-check">
                <input class="form-check-input option-checkbox" type="checkbox" value="${value}">
                <label class="form-check-label">${text}</label>
              </div>
            </div>`;
    }

    function collectFilters(excludeKey = null) {
        const filters = {};
        $('.multi-select-dropdown').each(function() {
            const key = $(this).data('filter-key');
            if (key !== excludeKey) {
                const values = [];
                $(this).find('.option-checkbox:checked').each(function() {
                    values.push($(this).val());
                });
                if (values.length > 0) {
                    filters[key] = values;
                }
            }
        });
        return filters;
    }

    function getCombinedFilters() {
        const filters = collectFilters();
        
        // 添加项目ID
        const projectIds = [];
        $('.project-checkbox:checked').each(function() {
            projectIds.push($(this).val());
        });
        filters.projects = projectIds;

        // 添加时间筛选
        filters.production_start_month = $('#production_start_month').val();
        filters.production_end_month = $('#production_end_month').val();
        filters.transaction_start_date = $('#transaction_start_date').val();
        filters.transaction_end_date = $('#transaction_end_date').val();

        return filters;
    }
    
    // ---- 以下是原有的函数，稍作修改或保留 ----

    function fetchAnalysisData() {
        const filters = getCombinedFilters();
        console.log("Fetching analysis data with filters:", filters);
        $('#loading').show();
        $('#resultBody').empty().append('<tr><td colspan="26" class="text-center text-muted">加载中...</td></tr>');
      
        $.ajax({
            url: '{{ url_for("dashboard.get_analysis_data") }}',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify(filters),
            success: function(response) {
          console.log('获取到分析数据:', response);
          $('#resultBody').empty();
          
          if (response && response.length > 0) {
            // 创建汇总数据对象
            let summaryData = {
              ordinary_green: 0,
              green_green: 0,
              issued_platform_sold: 0,
              unilateral_qty: 0,
              unilateral_amt: 0,
              online_qty: 0,
              online_amt: 0,
              offline_qty: 0,
              offline_amt: 0,
              beijing_qty: 0,
              beijing_amt: 0,
              guangzhou_qty: 0,
              guangzhou_amt: 0
            };
            
            // 先添加汇总行
            const summaryRow = `
              <tr class="table-secondary fw-bold">
                <td>汇总</td>
                <td id="sum_total_issued">0</td>
                <td id="sum_ordinary_green">0</td>
                <td id="sum_green_green">0</td>
                <td id="sum_issued_platform_sold">0</td>
                <td id="sum_inventory">0</td>
                <td id="sum_unilateral_qty" class="col-qty">0</td>
                <td id="sum_unilateral_amt" class="col-amt">0</td>
                <td id="sum_unilateral_avg" class="col-avg">0</td>
                <td id="sum_online_qty" class="col-qty">0</td>
                <td id="sum_online_amt" class="col-amt">0</td>
                <td id="sum_online_avg" class="col-avg">0</td>
                <td id="sum_offline_qty" class="col-qty">0</td>
                <td id="sum_offline_amt" class="col-amt">0</td>
                <td id="sum_offline_avg" class="col-avg">0</td>
                <td id="sum_beijing_qty" class="col-qty">0</td>
                <td id="sum_beijing_amt" class="col-amt">0</td>
                <td id="sum_beijing_avg" class="col-avg">0</td>
                <td id="sum_guangzhou_qty" class="col-qty">0</td>
                <td id="sum_guangzhou_amt" class="col-amt">0</td>
                <td id="sum_guangzhou_avg" class="col-avg">0</td>
                <td id="sum_total_qty" class="col-qty">0</td>
                <td id="sum_total_amt" class="col-amt">0</td>
                <td id="sum_total_avg" class="col-avg">0</td>
                <td id="sum_issued_ratio">0.0%</td>
                <td id="sum_trading_ratio">0.0%</td>
              </tr>
            `;
            $('#resultBody').append(summaryRow);
            
            // 添加各月份数据行并累计汇总数据
            response.forEach(function(row) {
              // 计算交易平台售出汇总
              const totalQty = (parseFloat(row.unilateral_qty) || 0) + 
                              (parseFloat(row.online_qty) || 0) + 
                              (parseFloat(row.offline_qty) || 0) + 
                              (parseFloat(row.beijing_qty) || 0) + 
                              (parseFloat(row.guangzhou_qty) || 0);
              
              const totalAmt = (parseFloat(row.unilateral_amt) || 0) + 
                              (parseFloat(row.online_amt) || 0) + 
                              (parseFloat(row.offline_amt) || 0) + 
                              (parseFloat(row.beijing_amt) || 0) + 
                              (parseFloat(row.guangzhou_amt) || 0);
              
              const totalAvg = totalQty > 0 ? (totalAmt / totalQty).toFixed(2) : 0;
              
              // 计算总核发量
              const ordinaryGreen = parseFloat(row.ordinary_green) || 0;
              const greenGreen = parseFloat(row.green_green) || 0;
              const totalIssued = ordinaryGreen + greenGreen;
              
              // 计算库存量
              const inventory = ordinaryGreen - (parseFloat(row.issued_platform_sold) || 0);
              
              // 计算售出比例
              const issuedRatio = ordinaryGreen > 0 ? ((parseFloat(row.issued_platform_sold) || 0) / ordinaryGreen * 100).toFixed(1) + '%' : '0.0%';
              const tradingRatio = ordinaryGreen > 0 ? (totalQty / ordinaryGreen * 100).toFixed(1) + '%' : '0.0%';
              
              const tr = `
                <tr>
                  <td>${row.production_year_month || ''}</td>
                  <td>${Math.round(totalIssued)}</td>
                  <td>${row.ordinary_green || 0}</td>
                  <td>${row.green_green || 0}</td>
                  <td>${row.issued_platform_sold || 0}</td>
                  <td>${Math.round(inventory)}</td>
                  <td class="col-qty">${row.unilateral_qty || 0}</td>
                  <td class="col-amt">${row.unilateral_amt || 0}</td>
                  <td class="col-avg">${row.unilateral_avg || 0}</td>
                  <td class="col-qty">${row.online_qty || 0}</td>
                  <td class="col-amt">${row.online_amt || 0}</td>
                  <td class="col-avg">${row.online_avg || 0}</td>
                  <td class="col-qty">${row.offline_qty || 0}</td>
                  <td class="col-amt">${row.offline_amt || 0}</td>
                  <td class="col-avg">${row.offline_avg || 0}</td>
                  <td class="col-qty">${row.beijing_qty || 0}</td>
                  <td class="col-amt">${row.beijing_amt || 0}</td>
                  <td class="col-avg">${row.beijing_avg || 0}</td>
                  <td class="col-qty">${row.guangzhou_qty || 0}</td>
                  <td class="col-amt">${row.guangzhou_amt || 0}</td>
                  <td class="col-avg">${row.guangzhou_avg || 0}</td>
                  <td class="col-qty">${Math.round(totalQty)}</td>
                  <td class="col-amt">${totalAmt.toFixed(2)}</td>
                  <td class="col-avg">${totalAvg}</td>
                  <td>${issuedRatio}</td>
                  <td>${tradingRatio}</td>
                </tr>
              `;
              $('#resultBody').append(tr);
              
              // 累加汇总数据
              summaryData.ordinary_green += parseFloat(row.ordinary_green) || 0;
              summaryData.green_green += parseFloat(row.green_green) || 0;
              summaryData.issued_platform_sold += parseFloat(row.issued_platform_sold) || 0;
              summaryData.unilateral_qty += parseFloat(row.unilateral_qty) || 0;
              summaryData.unilateral_amt += parseFloat(row.unilateral_amt) || 0;
              summaryData.online_qty += parseFloat(row.online_qty) || 0;
              summaryData.online_amt += parseFloat(row.online_amt) || 0;
              summaryData.offline_qty += parseFloat(row.offline_qty) || 0;
              summaryData.offline_amt += parseFloat(row.offline_amt) || 0;
              summaryData.beijing_qty += parseFloat(row.beijing_qty) || 0;
              summaryData.beijing_amt += parseFloat(row.beijing_amt) || 0;
              summaryData.guangzhou_qty += parseFloat(row.guangzhou_qty) || 0;
              summaryData.guangzhou_amt += parseFloat(row.guangzhou_amt) || 0;
            });
            
            // 计算汇总行的均价和比例
            const sumUnilateralAvg = summaryData.unilateral_qty > 0 ? (summaryData.unilateral_amt / summaryData.unilateral_qty).toFixed(2) : 0;
            const sumOnlineAvg = summaryData.online_qty > 0 ? (summaryData.online_amt / summaryData.online_qty).toFixed(2) : 0;
            const sumOfflineAvg = summaryData.offline_qty > 0 ? (summaryData.offline_amt / summaryData.offline_qty).toFixed(2) : 0;
            const sumBeijingAvg = summaryData.beijing_qty > 0 ? (summaryData.beijing_amt / summaryData.beijing_qty).toFixed(2) : 0;
            const sumGuangzhouAvg = summaryData.guangzhou_qty > 0 ? (summaryData.guangzhou_amt / summaryData.guangzhou_qty).toFixed(2) : 0;
            
            // 计算交易平台售出汇总
            const sumTotalQty = summaryData.unilateral_qty + summaryData.online_qty + summaryData.offline_qty + summaryData.beijing_qty + summaryData.guangzhou_qty;
            const sumTotalAmt = summaryData.unilateral_amt + summaryData.online_amt + summaryData.offline_amt + summaryData.beijing_amt + summaryData.guangzhou_amt;
            const sumTotalAvg = sumTotalQty > 0 ? (sumTotalAmt / sumTotalQty).toFixed(2) : 0;
            
            // 计算售出比例
            const sumIssuedRatio = summaryData.ordinary_green > 0 ? ((summaryData.issued_platform_sold / summaryData.ordinary_green) * 100).toFixed(1) + '%' : '0.0%';
            const sumTradingRatio = summaryData.ordinary_green > 0 ? ((sumTotalQty / summaryData.ordinary_green) * 100).toFixed(1) + '%' : '0.0%';
            
            // 计算总核发量汇总
            const sumTotalIssued = summaryData.ordinary_green + summaryData.green_green;
            
            // 计算库存量汇总
            const sumInventory = summaryData.ordinary_green - summaryData.issued_platform_sold;
            
            // 更新汇总行的数据
            $('#sum_total_issued').text(Math.round(sumTotalIssued));
            $('#sum_ordinary_green').text(Math.round(summaryData.ordinary_green));
            $('#sum_green_green').text(Math.round(summaryData.green_green));
            $('#sum_issued_platform_sold').text(Math.round(summaryData.issued_platform_sold));
            $('#sum_inventory').text(Math.round(sumInventory));
            $('#sum_unilateral_qty').text(Math.round(summaryData.unilateral_qty));
            $('#sum_unilateral_amt').text(summaryData.unilateral_amt.toFixed(2));
            $('#sum_unilateral_avg').text(sumUnilateralAvg);
            $('#sum_online_qty').text(Math.round(summaryData.online_qty));
            $('#sum_online_amt').text(summaryData.online_amt.toFixed(2));
            $('#sum_online_avg').text(sumOnlineAvg);
            $('#sum_offline_qty').text(Math.round(summaryData.offline_qty));
            $('#sum_offline_amt').text(summaryData.offline_amt.toFixed(2));
            $('#sum_offline_avg').text(sumOfflineAvg);
            $('#sum_beijing_qty').text(Math.round(summaryData.beijing_qty));
            $('#sum_beijing_amt').text(summaryData.beijing_amt.toFixed(2));
            $('#sum_beijing_avg').text(sumBeijingAvg);
            $('#sum_guangzhou_qty').text(Math.round(summaryData.guangzhou_qty));
            $('#sum_guangzhou_amt').text(summaryData.guangzhou_amt.toFixed(2));
            $('#sum_guangzhou_avg').text(sumGuangzhouAvg);
            $('#sum_total_qty').text(Math.round(sumTotalQty));
            $('#sum_total_amt').text(sumTotalAmt.toFixed(2));
            $('#sum_total_avg').text(sumTotalAvg);
            $('#sum_issued_ratio').text(sumIssuedRatio);
            $('#sum_trading_ratio').text(sumTradingRatio);
          } else {
            $('#resultBody').append('<tr><td colspan="26" class="text-center text-muted">暂无数据</td></tr>');
          }

          updateColumnVisibility();
          $('#loading').hide();
        },
        error: function(error) {
          console.error('获取分析数据失败:', error);
          $('#resultBody').append('<tr><td colspan="26" class="text-center text-danger">数据加载失败，请重试</td></tr>');
          $('#loading').hide();
        }
        });
    }

    function fetchTransactionTimeData() {
        const filters = getCombinedFilters();
        console.log("Fetching transaction time data with filters:", filters);
        $('#loading').show();
        $('#transactionBody').empty().append('<tr><td colspan="19" class="text-center text-muted">加载中...</td></tr>');
        
        $.ajax({
            url: '{{ url_for("dashboard.get_transaction_time_data") }}',
            type: 'POST',
            contentType: 'application/json',
            data: JSON.stringify(filters),
            success: function(response) {
          console.log('获取到交易时间汇总数据:', response);
          $('#transactionBody').empty();
          
          if (response && response.length > 0) {
            // 创建汇总数据对象
            let summaryData = {
              unilateral_qty: 0,
              unilateral_amt: 0,
              online_qty: 0,
              online_amt: 0,
              offline_qty: 0,
              offline_amt: 0,
              beijing_qty: 0,
              beijing_amt: 0,
              guangzhou_qty: 0,
              guangzhou_amt: 0
            };
            
            // 先添加汇总行
            const summaryRow = `
              <tr class="table-secondary fw-bold">
                <td>汇总</td>
                <td id="t_sum_unilateral_qty" class="col-qty">0</td>
                <td id="t_sum_unilateral_amt" class="col-amt">0</td>
                <td id="t_sum_unilateral_avg" class="col-avg">0</td>
                <td id="t_sum_online_qty" class="col-qty">0</td>
                <td id="t_sum_online_amt" class="col-amt">0</td>
                <td id="t_sum_online_avg" class="col-avg">0</td>
                <td id="t_sum_offline_qty" class="col-qty">0</td>
                <td id="t_sum_offline_amt" class="col-amt">0</td>
                <td id="t_sum_offline_avg" class="col-avg">0</td>
                <td id="t_sum_beijing_qty" class="col-qty">0</td>
                <td id="t_sum_beijing_amt" class="col-amt">0</td>
                <td id="t_sum_beijing_avg" class="col-avg">0</td>
                <td id="t_sum_guangzhou_qty" class="col-qty">0</td>
                <td id="t_sum_guangzhou_amt" class="col-amt">0</td>
                <td id="t_sum_guangzhou_avg" class="col-avg">0</td>
                <td id="t_sum_total_qty" class="col-qty">0</td>
                <td id="t_sum_total_amt" class="col-amt">0</td>
                <td id="t_sum_total_avg" class="col-avg">0</td>
              </tr>
            `;
            $('#transactionBody').append(summaryRow);
            
            // 添加各月份数据行并累计汇总数据
            response.forEach(function(row) {
              // 计算交易平台售出汇总
              const totalQty = (parseFloat(row.unilateral_qty) || 0) + 
                              (parseFloat(row.online_qty) || 0) + 
                              (parseFloat(row.offline_qty) || 0) + 
                              (parseFloat(row.beijing_qty) || 0) + 
                              (parseFloat(row.guangzhou_qty) || 0);
              
              const totalAmt = (parseFloat(row.unilateral_amt) || 0) + 
                              (parseFloat(row.online_amt) || 0) + 
                              (parseFloat(row.offline_amt) || 0) + 
                              (parseFloat(row.beijing_amt) || 0) + 
                              (parseFloat(row.guangzhou_amt) || 0);
              
              const totalAvg = totalQty > 0 ? (totalAmt / totalQty).toFixed(2) : 0;
              
              const tr = `
                <tr>
                  <td>${row.transaction_year_month || ''}</td>
                  <td class="col-qty">${row.unilateral_qty || 0}</td>
                  <td class="col-amt">${row.unilateral_amt || 0}</td>
                  <td class="col-avg">${row.unilateral_avg || 0}</td>
                  <td class="col-qty">${row.online_qty || 0}</td>
                  <td class="col-amt">${row.online_amt || 0}</td>
                  <td class="col-avg">${row.online_avg || 0}</td>
                  <td class="col-qty">${row.offline_qty || 0}</td>
                  <td class="col-amt">${row.offline_amt || 0}</td>
                  <td class="col-avg">${row.offline_avg || 0}</td>
                  <td class="col-qty">${row.beijing_qty || 0}</td>
                  <td class="col-amt">${row.beijing_amt || 0}</td>
                  <td class="col-avg">${row.beijing_avg || 0}</td>
                  <td class="col-qty">${row.guangzhou_qty || 0}</td>
                  <td class="col-amt">${row.guangzhou_amt || 0}</td>
                  <td class="col-avg">${row.guangzhou_avg || 0}</td>
                  <td class="col-qty">${Math.round(totalQty)}</td>
                  <td class="col-amt">${totalAmt.toFixed(2)}</td>
                  <td class="col-avg">${totalAvg}</td>
                </tr>
              `;
              $('#transactionBody').append(tr);
              
              // 累加汇总数据
              summaryData.unilateral_qty += parseFloat(row.unilateral_qty) || 0;
              summaryData.unilateral_amt += parseFloat(row.unilateral_amt) || 0;
              summaryData.online_qty += parseFloat(row.online_qty) || 0;
              summaryData.online_amt += parseFloat(row.online_amt) || 0;
              summaryData.offline_qty += parseFloat(row.offline_qty) || 0;
              summaryData.offline_amt += parseFloat(row.offline_amt) || 0;
              summaryData.beijing_qty += parseFloat(row.beijing_qty) || 0;
              summaryData.beijing_amt += parseFloat(row.beijing_amt) || 0;
              summaryData.guangzhou_qty += parseFloat(row.guangzhou_qty) || 0;
              summaryData.guangzhou_amt += parseFloat(row.guangzhou_amt) || 0;
            });
            
            // 计算汇总行的均价
            const sumUnilateralAvg = summaryData.unilateral_qty > 0 ? (summaryData.unilateral_amt / summaryData.unilateral_qty).toFixed(2) : 0;
            const sumOnlineAvg = summaryData.online_qty > 0 ? (summaryData.online_amt / summaryData.online_qty).toFixed(2) : 0;
            const sumOfflineAvg = summaryData.offline_qty > 0 ? (summaryData.offline_amt / summaryData.offline_qty).toFixed(2) : 0;
            const sumBeijingAvg = summaryData.beijing_qty > 0 ? (summaryData.beijing_amt / summaryData.beijing_qty).toFixed(2) : 0;
            const sumGuangzhouAvg = summaryData.guangzhou_qty > 0 ? (summaryData.guangzhou_amt / summaryData.guangzhou_qty).toFixed(2) : 0;
            
            // 计算交易平台售出汇总
            const sumTotalQty = summaryData.unilateral_qty + summaryData.online_qty + summaryData.offline_qty + summaryData.beijing_qty + summaryData.guangzhou_qty;
            const sumTotalAmt = summaryData.unilateral_amt + summaryData.online_amt + summaryData.offline_amt + summaryData.beijing_amt + summaryData.guangzhou_amt;
            const sumTotalAvg = sumTotalQty > 0 ? (sumTotalAmt / sumTotalQty).toFixed(2) : 0;
            
            // 更新汇总行的数据
            $('#t_sum_unilateral_qty').text(Math.round(summaryData.unilateral_qty));
            $('#t_sum_unilateral_amt').text(summaryData.unilateral_amt.toFixed(2));
            $('#t_sum_unilateral_avg').text(sumUnilateralAvg);
            $('#t_sum_online_qty').text(Math.round(summaryData.online_qty));
            $('#t_sum_online_amt').text(summaryData.online_amt.toFixed(2));
            $('#t_sum_online_avg').text(sumOnlineAvg);
            $('#t_sum_offline_qty').text(Math.round(summaryData.offline_qty));
            $('#t_sum_offline_amt').text(summaryData.offline_amt.toFixed(2));
            $('#t_sum_offline_avg').text(sumOfflineAvg);
            $('#t_sum_beijing_qty').text(Math.round(summaryData.beijing_qty));
            $('#t_sum_beijing_amt').text(summaryData.beijing_amt.toFixed(2));
            $('#t_sum_beijing_avg').text(sumBeijingAvg);
            $('#t_sum_guangzhou_qty').text(Math.round(summaryData.guangzhou_qty));
            $('#t_sum_guangzhou_amt').text(summaryData.guangzhou_amt.toFixed(2));
            $('#t_sum_guangzhou_avg').text(sumGuangzhouAvg);
            $('#t_sum_total_qty').text(Math.round(sumTotalQty));
            $('#t_sum_total_amt').text(sumTotalAmt.toFixed(2));
            $('#t_sum_total_avg').text(sumTotalAvg);
          } else {
            $('#transactionBody').append('<tr><td colspan="19" class="text-center text-muted">暂无数据</td></tr>');
          }
          
          updateColumnVisibility();
          $('#loading').hide();
        },
        error: function(error) {
          console.error('获取交易时间汇总数据失败:', error);
          $('#transactionBody').append('<tr><td colspan="19" class="text-center text-danger">数据加载失败，请重试</td></tr>');
          $('#loading').hide();
        }
        });
    }

    function updateColumnVisibility() {
        const showQty = $('#toggleQty').is(':checked');
        const showAmt = $('#toggleAmt').is(':checked');
        const showAvg = $('#toggleAvg').is(':checked');

        // Toggle individual columns
        $('.col-qty').toggle(showQty);
        $('.col-amt').toggle(showAmt);
        $('.col-avg').toggle(showAvg);

        // Update colspan for parent headers and visibility
        $('.col-parent-group').each(function() {
            let colspan = 0;
            if (showQty) colspan++;
            if (showAmt) colspan++;
            if (showAvg) colspan++;
            
            if (colspan > 0) {
                $(this).show();
                $(this).attr('colspan', colspan);
            } else {
                $(this).hide();
            }
        });
    }
    
    function initProjectSelector() {
        // 初始化时全选所有项目
        $('.project-checkbox').prop('checked', true);
        updateSelectedProjects();
        updateSelectAllState();
        
        // 搜索功能
        $('#projectSearch').on('input', function() {
            const searchTerm = $(this).val().toLowerCase();
            $('.project-item').each(function() {
                const projectName = $(this).find('label').text().toLowerCase();
                if (projectName.includes(searchTerm)) {
                    $(this).show();
                } else {
                    $(this).hide();
                }
            });
        });
        
        // 全选功能
        $('#selectAllProjects').change(function() {
            const isChecked = $(this).is(':checked');
            $('.project-checkbox:visible').prop('checked', isChecked);
            updateSelectedProjects();
        });
        
        // 项目复选框变化
        $('.project-checkbox').change(function() {
            updateSelectedProjects();
            updateSelectAllState();
        });
        
        // 防止下拉菜单关闭
        $('.dropdown-menu').on('click', function(e) {
            e.stopPropagation();
        });
    }
});