/**
 * transform data from hive using hive shell command
 * @author: sunfang1cn@yahoo.com.cn
 */

var ps = require('procstreams');
var Stream = require('stream').Stream;

var adaptor = {};

adaptor['json'] = function (input, rs, cs) {
  var row = input.split(rs);
  if (row.length < 2) {
    return null;
  }
  var header = row[0].split(cs);
  if (header.length < 1) {
    return null;
  }
  var output = [];
  for (var i = 1, l = row.length; i < l; i++) {
    var col = row.split(cs);
    if (col.length === header.length) {
      for (var j = 0; j < col.length; j++) {
        output[i - 1] = output[i - 1] || {};
        output[i - 1][header[i]] = col[i];
      }
    }
  }
  return output;
};

adaptor['buffer'] = function (input, rs, cs) {
  var row = input.split(rs);
  if (row.length < 2) {
    return null;
  }
  var header = row[0].split(cs);
  if (header.length < 1) {
    return null;
  }
  var output = new Buffer(row.slice(1).join(rs).toString());
  return output;
};

adaptor['array'] = function (input, rs, cs) {
  var row = input.split(rs);
  if (row.length < 2) {
    return null;
  }
  var header = row[0].split(cs);
  if (header.length < 1) {
    return null;
  }
  var output = [];
  for (var i = 1, l = row.length; i < l; i++) {
    var col = row.split(cs);
    if (col.length === header.length) {
      output.push(col);
    }
  }
  return output;
};

module.exports = function (conf) {
  conf = conf || {};
  conf.output = conf.output || 'json';
  conf.rowSplit = conf.rowSplit || '\n';
  conf.colSplit = conf.colSplit || '\t';

  var job = 0,
    totalJob = 0,
    map = 0,
    red = 0;

  if (typeof adaptor[conf.output] !== 'function') {
    throw new Error('Hiveshell: Adaptor not found.');
    return null;
  }

  this.prototype.getState = function () {
    return {currentJob: job, totalJob: totalJob, mapComplate: map, reduceComplate: red};
  }

  this.prototype.excute = function (sql, args, cb) {
    if (!sql) {
      return cb(new Error('sql must exists.'));
    }
    for (var key in args) {
      var reg = new RegExp(":" + key, "ig");
      sql = sql.replace(reg, '\'' + (args[key] || '0') + '\'');
    }
    if (sql.indexOf('\:') >= 0) {
      return cb(new Error('Hiveshell: some args is leak.'));
    }
    sql = 'set hive.cli.print.header=true;' + sql;
    var startTime = new Date().getTime();
    job = 0;
    totalJob = 0;
    map = 0;
    red = 0;
    var statusData = '';
    var summary = new Stream();
    summary.readable = true;
    summary.writable = true;
    summary.on('data', function (data) {
      data = data.toString();
      statusData += data;
      if (statusData.indexOf('Time taken:') >= 0) {
        return;
      }
      var lti = statusData.lastIndexOf('Launching Job');
      if (lti > 0) {
        var jobdet = statusData.substr(lti, statusData.indexOf('\n', lti) - 1).split(' ');
        job = jobdet[2];
        totalJob = jobdet[5];
      }
      lti = statusData.lastIndexOf('map = ');
      if (lti > 0) {
        var mrdet = statusData.substr(lti, statusData.indexOf('\n', lti) - 1).split(' ');
        job = jobdet[2].slice(-2);
        totalJob = jobdet[5].slice(-1);
      }
    });
    var proc = ps('hive -e \"' + sql + '\"').data(function (err, stdout, stderr) {
      if (err) {
        return cb(new Error("Hiveshell: Hive Excute Error! %s", stderr.toString()));
      }
      var info = stderr.toString();
      if (info.indexOf('OK') <= 0 || info.indexOf('Time taken:') <= 0) {
        return cb(new Error("Hiveshell: Hive Get Results Error!"));
      }
      var out = stdout.toString();
      return callback(null, adaptor[conf.output](out, conf.rowSplit, conf.colSplit));
    });
    proc.pipe(summary);
  }
  return this;
};

