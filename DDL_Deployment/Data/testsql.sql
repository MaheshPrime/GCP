create table `<project>.<dataset>.test_table1`
(
subs_id           string               not null options (description = "dummy description"),
sub_subs_id       string               not null options (description = "dummy description"),
cust_acct_id      string               not null options (description = "dummy description"),
sub_cust_acct_id  string               not null options (description = "dummy description"),
workforce_id      string               not null options (description = "dummy description"),
sub_workforce_id  string               not null options (description = "dummy description"),
priceable_item_id string               not null options (description = "dummy description"),
debit_credit_reas_id string               not null options (description = "dummy description"),
comments string                options (description = "dummy description"),
extended_attr_1 string                options (description = "dummy description"),
extended_attr_2 string                options (description = "dummy description"),
extended_attr_3 string                options (description = "dummy description"),
extended_attr_4 string                options (description = "dummy description"),
extended_attr_5 string                options (description = "dummy description"),
extended_attr_6 string                options (description = "dummy description"),
extended_attr_7 string                options (description = "dummy description"),
extended_attr_8 string                options (description = "dummy description"),
extended_attr_9 string                options (description = "dummy description"),
extended_attr_10 string                options (description = "dummy description"),
extraction_dttm timestamp               not null options (description = "dummy description"),
load_dttm timestamp               not null options (description = "dummy description"),
update_dttm timestamp               not null options (description = "dummy description"),
insert_load_id int64               not null options (description = "dummy description"),
update_load_id int64               not null options (description = "dummy description")
)
options
(
        labels = [("lm", "<local_mkt>"), ("env","<env_var>")],
        description = "dummy description ",
        friendly_name = "friendly_name"
);
create table `<project>.<dataset>.test_table2`
(
subs_id           string               not null options (description = "dummy description"),
sub_subs_id       string               not null options (description = "dummy description"),
cust_acct_id      string               not null options (description = "dummy description"),
sub_cust_acct_id  string               not null options (description = "dummy description"),
workforce_id      string               not null options (description = "dummy description"),
sub_workforce_id  string               not null options (description = "dummy description"),
priceable_item_id string               not null options (description = "dummy description"),
debit_credit_reas_id string               not null options (description = "dummy description"),
comments string                options (description = "dummy description"),
extended_attr_1 string                options (description = "dummy description"),
extended_attr_2 string                options (description = "dummy description"),
extended_attr_3 string                options (description = "dummy description"),
extended_attr_4 string                options (description = "dummy description"),
extended_attr_5 string                options (description = "dummy description"),
extended_attr_6 string                options (description = "dummy description"),
extended_attr_7 string                options (description = "dummy description"),
extended_attr_8 string                options (description = "dummy description"),
extended_attr_9 string                options (description = "dummy description"),
extended_attr_10 string                options (description = "dummy description"),
extraction_dttm timestamp               not null options (description = "dummy description"),
load_dttm timestamp               not null options (description = "dummy description"),
update_dttm timestamp               not null options (description = "dummy description"),
insert_load_id int64               not null options (description = "dummy description"),
update_load_id int64               not null options (description = "dummy description")
)
options
(
        labels = [("lm", "<local_mkt>"), ("env","<env_var>")],
        description = "dummy description ",
        friendly_name = "friendly_name"
);