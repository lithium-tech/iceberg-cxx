# distinct
time stats/stats_main --filename $FILEPATH --nouse_dictionary_optimization --nouse_string_view_heuristic --evaluate_distinct --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/distinct.txt
time stats/stats_main --filename $FILEPATH --nouse_dictionary_optimization --use_string_view_heuristic --evaluate_distinct --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/distinct_sv.txt
time stats/stats_main --filename $FILEPATH --use_dictionary_optimization --nouse_string_view_heuristic --evaluate_distinct --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/distinct_dict.txt
time stats/stats_main --filename $FILEPATH --use_dictionary_optimization --use_string_view_heuristic --evaluate_distinct --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/distinct_dict_sv.txt

# frequent items
time stats/stats_main --filename $FILEPATH --nouse_dictionary_optimization --nouse_precalculation_optimization --nouse_string_view_heuristic --evaluate_frequent_items --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/fi.txt
time stats/stats_main --filename $FILEPATH --nouse_dictionary_optimization --nouse_precalculation_optimization --nouse_string_view_heuristic --evaluate_frequent_items --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/fi_nodict.txt
time stats/stats_main --filename $FILEPATH --use_dictionary_optimization --use_precalculation_optimization --use_string_view_heuristic --evaluate_frequent_items --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/fi_dict_prec.txt

# quantiles
time stats/stats_main --filename $FILEPATH --nouse_dictionary_optimization --nouse_precalculation_optimization --nouse_string_view_heuristic --evaluate_quantiles --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/quantiles.txt
time stats/stats_main --filename $FILEPATH --use_dictionary_optimization --use_precalculation_optimization --use_string_view_heuristic --evaluate_quantiles --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/quantiles_dict_prec_sv.txt
time stats/stats_main --filename $FILEPATH --use_dictionary_optimization --use_precalculation_optimization --use_string_view_heuristic --evaluate_quantiles --print_timings 2>&1 | tee $CURRENT_RESULTS_DIR/quantiles_dict_prec_sv_patched.txt
