########################################################################################################################
cmake_minimum_required( VERSION 3.12 FATAL_ERROR )
find_package( ecbuild 3.7 REQUIRED HINTS ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../ecbuild)
project( GJBundle VERSION 0.0.1 LANGUAGES C CXX )
########################################################################################################################
macro( ecbuild_set_verbose )
    set( ${ARGV} )
    message( STATUS "SET ${ARGV0} = ${ARGV1}" )
endmacro()
########################################################################################################################
ecbuild_bundle_initialize()

ecbuild_bundle( PROJECT eccodes         GIT "git@github.com:ecmwf/eccodes"                   BRANCH develop  UPDATE)
ecbuild_bundle( PROJECT eckit           GIT "git@github.com:ecmwf/eckit"                     BRANCH develop  UPDATE)
ecbuild_bundle( PROJECT odc             GIT "git@github.com:ecmwf/odc"                       BRANCH develop UPDATE)
ecbuild_bundle( PROJECT metkit          GIT "git@github.com:ecmwf/metkit"                    BRANCH develop UPDATE)
ecbuild_bundle( PROJECT fdb5            GIT "git@github.com:ecmwf/fdb"                       BRANCH develop MANUAL)

ecbuild_bundle_finalize()