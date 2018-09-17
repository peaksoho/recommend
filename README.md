# recommend
Go语言版推荐算法程序，暂时只实现了LFM(Latent Factor Mode，潜在因子)算法

conf目录下的StorageCfg.json文件为数据库和Redis服务器配置文件。

执行方法：
* 单线程版 ./recommend lfm
* 多线程版 ./recommend lfm mp

lfm/initVisitor.go文件中所使用的数据表pms_product_visit_record必须有的字段：
<table>
<tr>
  <td>字段</td><td>类型</td><td>说明</td>
</tr>
<tr>  
  <td>visitor</td><td>String</td><td>访问者</td>
</tr>
<tr>
  <td>spu</td><td>Int</td><td>SPU</td>
</tr>
<tr>
  <td>sku</td><td>Int</td><td>SKU</td>
</tr>
<tr>
  <td>date</td><td>Date</td><td>访问日期</td>
</tr>
<tr>
  <td>weight</td><td>Int</td><td>操作权重</td>
</tr>
<tr>
  <td>click_num</td><td>Int</td><td>点击次数</td>
</tr>
<tr>
  <td>cart_num</td><td>Int</td><td>加购次数</td>
</tr>
<tr>
  <td>fav_num</td><td>Int</td><td>收藏次数</td>
</tr>
<tr>
  <td>order_num</td><td>Int</td><td>下单次数</td>
</tr>
</table>
