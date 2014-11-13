MultipleInputs is Broken
========================

Specifically it (very poorly) chose to use magic separator chars (',', ';')
to concat file paths, which prevents the use of glob paths. We have made a
short term work around that correctly encodes/decodes the paths to handle the
case where the path or glob contains the sparator chars. Longer term we will
build a better MultipleInputs, but until than this source patch will be used
to alleviate the issues seen in https://github.com/ambiata/ivory/issues/422.
