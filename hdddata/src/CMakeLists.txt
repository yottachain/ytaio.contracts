project(hdddata)

set(EOSIO_WASM_OLD_BEHAVIOR "Off")
find_package(eosio.cdt)

add_contract( hdddata hdddata hdddata.cpp )
target_include_directories( hdddata PUBLIC ${CMAKE_SOURCE_DIR}/../include )
target_ricardian_directory( hdddata ${CMAKE_SOURCE_DIR}/../ricardian )