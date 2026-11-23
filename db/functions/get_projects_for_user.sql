CREATE OR REPLACE FUNCTION bioprotect.get_projects_for_user(p_user_id integer)
 RETURNS TABLE(id integer, name text, description text, date_created timestamp without time zone, planning_unit_id integer, old_version boolean, iucn_category text, is_private boolean, costs text, default_resolution integer, planning_unit_alias text, role text)
 LANGUAGE plpgsql
 STABLE
AS $function$
BEGIN
    RETURN QUERY
    SELECT 
        p.id,
        p.name,
        p.description,
        p.date_created,
        p.planning_unit_id,
        p.old_version,
        p.iucn_category,
        p.is_private,
        p.costs,
        p.default_resolution,
        pu.alias AS planning_unit_alias,
        -- Admins see every project; where they aren't a member they act as
        -- editor (owner-only actions like delete/rename stay with the owner)
        COALESCE(up.role, 'editor')::text
    FROM bioprotect.projects p
    LEFT JOIN bioprotect.user_projects up
      ON up.project_id = p.id AND up.user_id = p_user_id
    LEFT JOIN bioprotect.metadata_planning_units pu
      ON p.planning_unit_id = pu.unique_id
    WHERE up.user_id IS NOT NULL
       OR EXISTS (SELECT 1 FROM bioprotect.users u
                  WHERE u.id = p_user_id AND u.role = 'Admin')
    ORDER BY LOWER(p.name);
END;
$function$

