cmake_minimum_required( VERSION 3.3 )
project( doxygen )
# used to compile documents

set( ${PROJECT_NAME}_VERSION 1.6 )
set( ${PROJECT_NAME}_SHA265 cedf78f6d213226464784ecb999b54515c97eab8a2f9b82514292f837cf88b93 )
set( ${PROJECT_NAME}_URL "http://ftp.stack.nl/pub/users/dimitri/doxygen-${${PROJECT_NAME}_VERSION}.src.tar.gz" )
#TODO flex broken set( ${PROJECT_NAME}_DEPENDS flex )

# Not a runtime requirement, so look for existing installation.
find_package(Doxygen ${${PROJECT_NAME}_VERSION} REQUIRED)
if( ${DOXYGEN_FOUND} )

	get_filename_component( DEPENDENCIES_${PROJECT_NAME}_DIR ${DOXYGEN_EXECUTABLE} DIRECTORY )
	get_filename_component( DEPENDENCIES_${PROJECT_NAME}_DIR ${DEPENDENCIES_${PROJECT_NAME}_DIR} DIRECTORY )
	set( DEPENDENCIES_${PROJECT_NAME}_DIR ${DEPENDENCIES_${PROJECT_NAME}_DIR} PARENT_SCOPE )
	set( DEPENDENCIES_${PROJECT_NAME}_VERSION ${DOXYGEN_VERSION} PARENT_SCOPE )	

else()
	# let's build it!
	# TODO FLEX won't build on Solaris
	# add_subdirectory( ../flex ${CMAKE_CURRENT_BINARY_DIR}/../flex )

	include(ExternalProject)

	# CMake based project
	ExternalProject_Add( ${PROJECT_NAME}
	   URL ${${PROJECT_NAME}_URL}
	   URL_HASH SHA256=${${PROJECT_NAME}_SHA265}
	   UPDATE_COMMAND ""
#	   DEPENDS ${${PROJECT_NAME}_DEPENDS}
	)

	ExternalProject_Get_Property( ${PROJECT_NAME} SOURCE_DIR )
	set( ${PROJECT_NAME}_SOURCE_DIR ${SOURCE_DIR} )
	ExternalProject_Get_Property( ${PROJECT_NAME} INSTALL_DIR )
	set( ${PROJECT_NAME}_INSTALL_DIR ${INSTALL_DIR} )

	set( DEPENDENCIES_${PROJECT_NAME}_DIR ${${PROJECT_NAME}_INSTALL_DIR} PARENT_SCOPE)
	set( DEPENDENCIES_${PROJECT_NAME}_VERSION ${${PROJECT_NAME}_VERSION} PARENT_SCOPE)

endif( )
