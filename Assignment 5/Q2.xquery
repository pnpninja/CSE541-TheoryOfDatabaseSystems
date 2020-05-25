let $products := distinct-values(doc("products.xml")/products/product/@pid)
for $product in $products
order by $product
return
    <totalcost partid="{$product}">
        {sum(doc("purchaseorders.xml")/PurchaseOrders/PurchaseOrder/item
        [partid=$product]
        /quantity) 
        * doc("products.xml")/products/product[@pid=$product]/description/price}
    </totalcost>