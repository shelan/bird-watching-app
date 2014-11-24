var express = require('express'),
    bodyParser = require('body-parser');

var app = express();
app.use(bodyParser());


var env = app.get('env') == 'development' ? 'dev' : app.get('env');
var port = process.env.PORT || 8080;


var Sequelize = require('sequelize');

// db config
var env = "dev";
var config = require('./database.json')[env];
var password = config.password ? config.password : null;

// initialize database connection
var sequelize = new Sequelize(
    config.database,
    config.user,
    config.password,
    {
        host: config.host,
        port: config.port,
        logging: console.log,
        define: {
            timestamps: false,
            freezeTableName: true
        }
    }
);

var DataTypes = require("sequelize");


//define sequelize
var Q1 = sequelize.define('Q1Table', {
    DATE: DataTypes.STRING,
    TOWER_ID: DataTypes.INTEGER,
    WING_SPAN: DataTypes.STRING
  }, {
    instanceMethods: {
      retrieveAll: function(onSuccess, onError) {
        Q1.findAll({attributes: ['DATE', 'TOWER_ID','WING_SPAN']}, {raw: true})
            .success(onSuccess).error(onError); 
      },
      retrieveByDate: function(date, onSuccess, onError) {
        Q1.find({attributes: ['DATE', 'TOWER_ID','WING_SPAN']},{where: {DATE: date}}, {raw: true})
            .success(onSuccess).error(onError); 
      }
    }
  }); 


var Q2 = sequelize.define('Q2Table', {
    DATE: DataTypes.STRING,
    TOWER_ID: DataTypes.INTEGER,
    TOTAL_WEIGHT: DataTypes.STRING
  }, {
    instanceMethods: {
      retrieveAll: function(onSuccess, onError) {
        Q2.findAll({attributes: ['DATE', 'TOWER_ID','TOTAL_WEIGHT']}, {raw: true})
            .success(onSuccess).error(onError); 
      },
      retrieveByDateAndTowerId: function(date, tower_id, onSuccess, onError) {
        Q2.find({attributes: ['DATE', 'TOWER_ID','TOTAL_WEIGHT']},{where: sequelize.and ({DATE: date},{TOWER_ID: tower_id })},{raw: true})
            .success(onSuccess).error(onError); 
      }
    }
  }); 


var Q3 = sequelize.define('Q3Table', {
    LAST_SEEN: DataTypes.STRING,
    BIRD_ID: DataTypes.INTEGER
  }, {
    instanceMethods: {
      retrieveAll: function(onSuccess, onError) {
        Q3.findAll({attributes: ['LAST_SEEN', 'BIRD_ID']}, {raw: true})
            .success(onSuccess).error(onError); 
      },
      retrieveByDate: function(date, onSuccess, onError) {
        Q3.find({where: {DATE: date}}, {raw: true})
            .success(onSuccess).error(onError); 
      }
    }
  }); 





// IMPORT ROUTES
// =============================================================================
var router = express.Router();

// on routes that end in /users
// ----------------------------------------------------
router.route('/q1/:date')


// get all the users (accessed at GET http://localhost:8080/birds)
.get(function(req, res) {
    
    sequelize
  .query(
    'SELECT * FROM Q1Table WHERE DATE= :date ', null,
    { raw: true }, {  date: req.params.date }
  )
  .success(function(birds) {
    res.send(birds);
  })

});


router.route('/q2/:date/:tower_id')

// get all the users (accessed at GET http://localhost:8080/birds)
.get(function(req, res) {

sequelize
  .query(
    'SELECT * FROM Q2Table WHERE TOWER_ID = :tower_id AND DATE= :date ', null,
    { raw: true }, { tower_id: req.params.tower_id, date: req.params.date }
  )
  .success(function(birds) {
    res.send(birds);
  })

});



router.route('/q3')

// get all the users (accessed at GET http://localhost:8080/birds)
.get(function(req, res) {

        var today = new Date();
        var lastWeek = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 7);

        sequelize
            .query(
            'SELECT * FROM Q3Table WHERE LAST_SEEN < '+ lastWeek.getTime() + ' AND BIRD_ID > -1', null,
            { raw: true }, { }
        )
            .success(function(birds) {
                res.send(birds);
            })

    });


router.route('/heartbeat')

// get all the users (accessed at GET http://localhost:8080/birds)
    .get(function(req, res) {

        sequelize
            .query(
            'SELECT 1', null,
            { raw: true }, { }
        )
            .success(function() {
                res.send('{alive}');
            })

    });



// Middleware to use for all requests
router.use(function(req, res, next) {
    // do logging
    console.log('Something is happening.');
    next();
});

app.use(express.static(__dirname + '/public'));


// REGISTER OUR ROUTES
// =============================================================================
app.use('/api', router);

// START THE SERVER
// =============================================================================
app.listen(port);
console.log('Magic happens on port ' + port);
