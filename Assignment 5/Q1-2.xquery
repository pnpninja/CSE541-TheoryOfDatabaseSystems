doc("customerinfo.xml")/customers/customerinfo[assistant/node()][addr/city/text() = "Toronto"]/(concat(name, if(position() eq last()) then "" else ","))