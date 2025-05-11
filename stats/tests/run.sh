# distinct
time stats/stats_main --filename $FILEPATH --nouse_dictionary_optimization --evaluate_distinct --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/distinct_nodict.txt
time stats/stats_main --filename $FILEPATH --use_dictionary_optimization --evaluate_distinct --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/distinct_dict.txt

# frequent items
time stats/stats_main --filename $FILEPATH --nouse_precalculation_optimization --nouse_dictionary_optimization --evaluate_frequent_items --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/fi_nodict.txt
time stats/stats_main --filename $FILEPATH --nouse_precalculation_optimization --use_dictionary_optimization --evaluate_frequent_items --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/fi_dict_noprecalc.txt
time stats/stats_main --filename $FILEPATH --use_precalculation_optimization --use_dictionary_optimization --evaluate_frequent_items --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/fi_dict_precalc.txt

# quantiles
time stats/stats_main --filename $FILEPATH --nouse_precalculation_optimization --nouse_dictionary_optimization --evaluate_quantiles --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/quantiles_nodict.txt
time stats/stats_main --filename $FILEPATH --use_precalculation_optimization --use_dictionary_optimization --evaluate_quantiles --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/quantiles_dict_precalc_sv.txt
