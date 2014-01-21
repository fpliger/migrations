import os
from mako.template import Template

here = os.path.dirname(os.path.abspath(__file__))
def html(filepath, context, templatepath=None):
    if templatepath is None:
        templatepath= os.path.join(here, "templates/html_report.mako")
    mytemplate = Template(filename=templatepath)
    with file(filepath, "w") as fp:
        fp.write(mytemplate.render(**context))