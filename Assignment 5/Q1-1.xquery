for $month in distinct-values(month-from-date(xs:date(doc("purchaseorders.xml")/PurchaseOrders/PurchaseOrder/string(@OrderDate))))
for $products :=doc("products.xml")/products/product/string(@pid)
let $items := doc("purchaseorders.xml")/PurchaseOrders/PurchaseOrder[month-from-date(xs:date(string(@OrderDate)))=$month]/item[partid=$products]
order by $month
return <count month="{$month}" partid="{$products}"> {count($items)}</count>
