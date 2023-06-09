CREATE INDEX "discount_proj_id_datetime" ON discount USING btree (id, "datetime") INCLUDE (discount);
CREATE INDEX "project_info_utility_region" ON project_info USING btree (utility, region);
CREATE INDEX "eavc_datetime_util_region" ON elec_av_costs USING btree ("datetime", utility, region) INCLUDE (total, marginal_ghg);
CREATE INDEX "eavc_util_region" ON elec_av_costs USING btree (utility, region);
CREATE INDEX "els_datetime_utility_name" ON elec_load_shape USING btree ("datetime", utility, name) INCLUDE (value);
