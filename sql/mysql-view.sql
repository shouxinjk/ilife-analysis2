

--字典视图：将dict_***均建立为视图
create view dict as 
select category,label,score,"dict_platform" as origin from dict_platform
union ALL
select category,label,score,"dict_brand" as origin from dict_brand
union ALL
select category,label,score,"dict_material" as origin from dict_material
union ALL
select category_id as category,original_value as label,marked_value as score,"ope_performance" as origin from ope_performance


--属性视图，包含直接属性及继承属性，包含商品及用户属性。其中用户属性category统一设置为user
CREATE VIEW property AS 
select DISTINCT c.name as property, c.category_id as origin_category,c.category_id,c.measure_id,a.auto_label_type,a.auto_label_dict,a.auto_label_category,a.auto_label_tag_category,a.normalize_type,a.multi_value_func,a.default_score,a.alpha,a.beta,a.gamma,a.delte,a.epsilon,a.zeta,a.eta,a.theta,a.lambda 
from mod_3rd_property c 
left join mod_measure a on a.id=c.measure_id 
where c.measure_id is not null
union ALL
select DISTINCT a.property as property, a.category as origin_category,c.id as category_id,a.id as measure_id,a.auto_label_type,a.auto_label_dict,a.auto_label_category,a.auto_label_tag_category,a.normalize_type,a.multi_value_func,a.default_score,a.alpha,a.beta,a.gamma,a.delte,a.epsilon,a.zeta,a.eta,a.theta,a.lambda 
from mod_item_category c 
right join mod_measure a on find_in_set(a.category,c.parent_ids)>0
union ALL
select DISTINCT a.property as property, a.category as origin_category,'user' as category_id,a.id as measure_id,a.auto_label_type,a.auto_label_dict,a.auto_label_category,a.auto_label_tag_category,a.normalize_type,a.multi_value_func,a.default_score,a.alpha,a.beta,a.gamma,a.delte,a.epsilon,a.zeta,a.eta,a.theta,a.lambda 
from mod_user_measure a


--商品评价维度视图：包括客观评价及主观评价，包含用户评价及商品评价
create view measure as 
select id,'0' as type,'item' as memo,category,propKey,featured,script,weight,parent_ids from mod_item_dimension
union ALL
select id,'1' as type,'item' as memo,category,propKey,featured,script,weight,parent_ids from mod_item_evaluation
union ALL
select id,'0' as type,'user' as memo,category,propKey,featured,script,weight,parent_ids from mod_user_dimension
union ALL
select id,'1' as type,'user' as memo,category,propKey,featured,script,weight,parent_ids from mod_user_evaluation