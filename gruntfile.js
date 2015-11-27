module.exports = function(grunt) {
 
  // Project configuration.
  grunt.initConfig({
    jsdoc : {
        dist : {
            src: ['lib/clients/*.js'],
            options: {
                destination: 'doc',
                configure: './jsdoc.conf',
                private: false,
                readme: './README.jsdoc'
                //template: './node_modules/ink-docstrap/template'
            }
        }
    }
  });
 
  grunt.loadNpmTasks('grunt-jsdoc');
 
};