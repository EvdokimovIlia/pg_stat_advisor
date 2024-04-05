# Copyright (c) 2022-2024, PostgreSQL Global Development Group

pg_stat_advisor_sources = files(
  'pg_stat_advisor.c',
)

if host_system == 'windows'
  pg_stat_advisor_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_stat_advsior',
    '--FILEDESC', 'pg_stat_advisor - suggestion of creating statistics',])
endif

pg_stat_advisor = shared_module('pg_stat_advisor',
  pg_stat_advisor_sources,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'],
  },
)
contrib_targets += pg_stat_advisor

tests += {
  'name': 'pg_stat_advisor',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'pg_stat_advisor',
    ],
  },
}
