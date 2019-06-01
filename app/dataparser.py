import xml.etree.ElementTree as ET

root = ET.parse('app/data/woodworking.stackexchange.com/Tags.xml').getroot()

for type_tag in root.findall('row'):
    tag_id = type_tag.get('Id')
    tag_name = type_tag.get('TagName')
    print(tag_id, tag_name)
