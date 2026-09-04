CREATE OR REPLACE FUNCTION bioprotect.touch_updated_at()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin new.updated_at = now(); return new; end $function$

