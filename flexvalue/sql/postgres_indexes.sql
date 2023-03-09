CREATE INDEX "discount_proj_id_timestamp" ON discount USING btree (project_id, "timestamp") INCLUDE (discount);
CREATE INDEX "project_info_utility_region" ON project_info USING btree (utility, region);
CREATE INDEX "eavc_timestamp_util_region" ON elec_av_costs USING btree ("timestamp", utility, region) INCLUDE (total, marginal_ghg);
CREATE INDEX "eavc_util_region" ON elec_av_costs USING btree (utility, region);
CREATE INDEX "els_timestamp_utility_name" ON elec_load_shape USING btree ("timestamp", utility, name) INCLUDE (value);
