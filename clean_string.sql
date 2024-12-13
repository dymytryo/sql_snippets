    split(regexp_replace(regexp_replace(trim(lower(name)), '\[|\]|'''), '(\w)(\w*)',
        x -> upper(x[1]) || lower(x[2])), ',')[1]   AS "Vendor Name",

