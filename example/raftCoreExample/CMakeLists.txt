set(SRC_LIST raftKvDB.cpp)


add_executable(raftCoreRun ${SRC_LIST})
target_link_libraries(raftCoreRun skip_list_on_raft rpc_lib protobuf muduo_net muduo_base pthread )

add_executable(raftNode raftNode.cpp)
target_link_libraries(raftNode PRIVATE skip_list_on_raft rpc_lib protobuf muduo_net muduo_base pthread)
add_test(NAME raft_node_help COMMAND $<TARGET_FILE:raftNode> --help)
set_tests_properties(raft_node_help PROPERTIES TIMEOUT 5)

add_executable(kvctl kvctl.cpp ${src_raftClerk} ${src_common})
target_link_libraries(kvctl PRIVATE skip_list_on_raft protobuf boost_serialization)
add_test(NAME kvctl_help COMMAND $<TARGET_FILE:kvctl> --help)
add_test(NAME kvctl_invalid COMMAND $<TARGET_FILE:kvctl> invalid)
set_tests_properties(kvctl_invalid PROPERTIES WILL_FAIL TRUE)
set_tests_properties(kvctl_help kvctl_invalid PROPERTIES TIMEOUT 5)

#################################

set(SRC_LIST2 caller.cpp)
add_executable(callerMain ${src_raftClerk} ${SRC_LIST2}  ${src_common})
target_link_libraries(callerMain skip_list_on_raft  protobuf boost_serialization )
