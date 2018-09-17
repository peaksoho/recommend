# recommend
Go语言版推荐算法程序，暂时只实现了LFM(Latent Factor Mode，潜在因子)算法

conf目录下的StorageCfg.json文件为数据库和Redis服务器配置文件。

执行方法：
* 单线程版 ./recommend lfm
* 多线程版 ./recommend lfm mp

数据表pms_product_visit_record必须有的字段：
<table>
<tr>
  <td>字段</td><td>类型</td><td>说明</td>
</tr>
</table>

visitor string 访问者
spu int 
,sku,date,weight,click_num,cart_num,fav_num,order_num
