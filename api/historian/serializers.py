from flask_restplus import fields
from api.restplus import api
from settings.initial_settings import SUPPORTED_FORMAT_DATES as time_formats

"""
    Configure the API HTML to show for each services the schemas that are needed 
    for posting and putting
    (Explain the arguments for each service)
"""

""" serializer for test_dict service """
test = api.model(
    'Value to post',
    {'value': fields.String(required=True, description='Any value to be stored using the test_id')})

test_out = api.inherit('Saved value', test, {
    'test_id': fields.String(description="String index"),
    'success': fields.Boolean(description="Successful operation")
})

""" serializers for tag service """
description = api.model("Attributes to describe an Entity",
                        {"description": fields.String(description="Description",
                                                      default="This is a description for this entity"),
                         "short_name": fields.String(description="Short name for the entity", default="New Element"),
                         "any_name": fields.String(description="One can add any attribute to work with",
                                                   default="Any attribute can be added")
                         })

tag = api.model("Create a new TagPoint",
                {'tag_name': fields.String(description="Unique TagName for identifying a TagPoint",
                                           required=True, default='newTagName'),
                 'tag_type': fields.String(description="Collection where the time series will be saved",
                                           default='generic'),
                 'attributes': fields.Nested(description)
                 })

tag_delete = api.model("Delete a TagPoint",
                       {'tag_name': fields.String(description="Unique TagName for identifying a TagPoint",
                                                  required=True, default='tag_name_to_delete'),
                        'tag_type': fields.String(description="Collection where the time series is saved",
                                                  required=True, default='analogs, events, generic, etc.')})

tag_update = api.inherit("Update name of a TagPoint", {
    "new_tag_name": fields.String(description="New tag name for a TagPoint",
                                  required=True, default="NewTagName")
})

""" serializers for group service """
group = api.model("Create a new groupPoint",
                  {'group_name': fields.String(description="Unique groupName for identifying a groupPoint",
                                               required=True, default='newGroupName'),
                   'group_type': fields.String(description="Type of group",
                                               default='generic'),
                   'tag_list': fields.List(fields.String(description="TagName"),
                                           default=["tag1", "tag2"], required=True),
                   'attributes': fields.Nested(description)})

group_delete = api.model("Delete a groupPoint",
                         {'group_name': fields.String(description="Unique groupName for identifying a groupPoint",
                                                      required=True, default='group_name_to_delete'),
                          'group_type': fields.String(description="Type of group. Ex: Device, Position",
                                                      required=True, default='device')})

group_update = api.inherit("Update name of a groupPoint", {
    "new_group_name": fields.String(description="New group name for a groupPoint",
                                    required=True, default="NewGroupName")
})

""" serializers for tag snap shoot service """

fmt_time = api.model("Format time",
                     {"format_time": fields.String(description="Time format options: " + str(time_formats),
                                                   required=True, default="%Y-%m-%d %H:%M:%S"
                                                   )})

register = api.model("Register", {
    "value": fields.Float(description="Value to save.", required=True),
    "timestamp": fields.String(descriptio="Timestamp",
                               required=True, default="yyyy-mm-dd H:M:S")
})

""" serializers for recorded_values service """

register_list = api.model("List of registers", {
    "registers": fields.List(fields.Nested(register))
})

""" serializers for registers """
register_tag_name = api.model("TagRegister", {
    "tag_name": fields.String(description="Tag_name for the TagPoint", required=True, default="tag_name_1"),
    "value": fields.Float(description="Value to save.", required=True, default=1.23),
    "timestamp": fields.String(description="Timestamp",
                               required=True, default="yyyy-mm-dd H:M:S")
})

register_tag_name_list = api.model("List of Tag registers", {
    "registers": fields.List(fields.Nested(register_tag_name))}
                                   )

tag_name_list = api.model("List of tag_names", {
    "tag_names": fields.List(fields.String, description="Tagnames", action='append',
                             required=True, default=["dev1.tag1", "dev2.tag2", "dev3.tag3"])}
                          )

""" Single list of records for a TagPoint"""
single_record_list = api.model("List of records for a TagPoint", {
    "tag_name": fields.String(description="Tag_name for the TagPoint", required=True, default="tag_name_1"),
    "registers": fields.List(fields.Nested(register))
})

""" serializer for recorded values """
register_for_tag_list = api.model("List of records for each TagPoint", {
    "list": fields.List(fields.Nested(single_record_list))
})
