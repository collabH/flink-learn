create table cdp_meta.tb_rule_mappings
(
    id             bigint auto_increment comment '主键'
        primary key,
    app_id         varchar(100) default ''                    not null comment '版本Id',
    org_id         varchar(100) default ''                    not null comment '租户Id',
    create_user_id varchar(100) default ''                    not null comment '创建人Id',
    create_time    timestamp    default CURRENT_TIMESTAMP     not null on update CURRENT_TIMESTAMP comment '创建时间',
    update_user_id varchar(100) default ''                    not null comment '创建人Id',
    update_time    timestamp    default '0000-00-00 00:00:00' not null on update CURRENT_TIMESTAMP comment '修改时间',
    deleted        tinyint(2)   default 0                     not null comment '删除状态，0：未删除；1：已删除',
    rule_id        bigint                                     not null comment '规则id',
    mapping_type   int          default 1                     not null comment '映射类型，1:tag,2:segment',
    mapping_id     bigint                                     not null comment '映射Id，规则id绑定的id',
    constraint idx_rule_id_mapping_id_mapping_type
        unique (rule_id, mapping_id, mapping_type, org_id)
)
    comment '规则映射表';