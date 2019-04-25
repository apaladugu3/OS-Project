const express = require('express');
const app = express();
const http = require('http').Server(app);
const bodyParser= require('body-parser')
const io = require('socket.io')(http);
const multer = require('multer');
const fs = require('fs');
const eventEmitter = require('events');
class MyEmitter extends eventEmitter{}
const myEmitter = new MyEmitter();
const Queue = require('queue-fifo');
const config = require('config');
const extractFrames = require('ffmpeg-extract-frames');
const ffmpegInstaller = require('@ffmpeg-installer/ffmpeg');
const ffmpeg = require('fluent-ffmpeg');
const List = require("collections/list");
const mkdirp = require('mkdirp');
const HashMap = require('hashmap');
const sqlite3 = require('sqlite3').verbose();
const rmdir = require('rimraf');
ffmpeg.setFfmpegPath(ffmpegInstaller.path);
module.exports = ffmpeg;
app.set('view engine', 'ejs');
app.use(bodyParser.urlencoded({extended: true}));

//From config file
const local_ip = config.get('Local.host');
const port = config.get('Local.port');
const local_path = config.get('Local.path');

//setting up the database
//const db = new sqlite3.Database('C:sqlite/comp512.db', sqlite3.OPEN_READWRITE, (err) => {
const db = new sqlite3.Database(local_path+'/comp512.db', sqlite3.OPEN_READWRITE, (err) => {
    if (err) {
        console.error(err.message);
    }
    console.log('Connected to the comp512 database.');
});


//creating various lists and queues for execution
//The state of the server
var currentframe= new HashMap();
var deleteJobs= new HashMap();
var activeJobs = new Queue();
var completedJobs = new Queue();
var activequeue = new Queue();
var activeclients= new HashMap();
var object = "giraffe";




//Creating storage for video files
const storage_vid = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, local_path+'/uploads/VideoUploads')
        mkdirp(local_path+'/uploads/Frames/'+req.ip, function(err) {

            if(err){
                   throw err;
            }
        })
    },
    filename: function (req, file, cb) {
        cb(null, req.ip + '.mp4')
    }
});

//Upload functions
const upload_vid = multer({ storage: storage_vid});

// upload video file
app.post('/uploadVidFile', upload_vid.single('videoFile'), (req, res) => {
    activequeue.enqueue(req.ip);
    currentframe.set(req.ip, 1);
    var target = req.body.targetObject;
    myEmitter.emit('vidUploaded',target);
    res.sendFile(__dirname + '/results.html');
});

//Event called after video is uploaded
myEmitter.on('vidUploaded', (target) => {
    console.log('video upload complete!');
    console.log('Target: ', target);
    var extract_ip = activequeue.dequeue()
    extract(extract_ip);
    db.each('Update Jobs SET object = ? where IP = ?', [target,extract_ip], (err, row) => {
        if (err) {
            throw err;
        }
        console.log(extract_ip + 'added');
    });
});

//function to split video into frames
async function extract(extract_ip) {
    //var extract_ip = activequeue.dequeue();
    try{
        await extractFrames({
            input: local_path+'/uploads/VideoUploads/'+extract_ip+'.mp4',
            output: local_path+'/uploads/Frames/'+extract_ip+'/'+extract_ip+'-%d.jpg',
            fps : 20,
        })
        rmdir.sync(local_path+'/uploads/VideoUploads/'+ extract_ip + '.mp4');
        fs.readdir(local_path+'/uploads/Frames/'+extract_ip, (err, files) => {
            totalFrames = files.length;
            console.log("Total number of frames: "+totalFrames);
            db.each('INSERT OR REPLACE INTO Jobs(IP, frames, c_frames,object) VALUES(?,?,1,NULL)', [extract_ip,totalFrames], (err, row) => {
                if (err) {
                    throw err;
                }
                console.log(extract_ip + 'added');
            });
        });
        if(deleteJobs.get(extract_ip)!=1)activeJobs.enqueue(extract_ip);
        console.log('Active job added for '+activeJobs.peek());
        console.log('video split successful')
    }
    catch (e) {
        console.log(e);
    }
}

//Index page
app.get('/', function(req, res){
    res.sendFile(__dirname + '/index.html');
    console.log(req.ip + ' connected!!');
});

//Returns the frame to process
app.get('/process', function(req, res){
    db.each('Select S_IP ip, frame frme from Clients where C_IP=?', [req.ip], (err, row) => {
        if (err) {
            throw err;
        }
            res.sendFile(__dirname + '/uploads/Frames/' + row.ip + '/' + row.ip+ '-' + row.frme + '.jpg');
    });
    
});

//Address that active clients hit to let know they want work
app.get('/processor', function(req, res) {
    //res.sendFile(__dirname + '/objDet.html');
    var pro_ip= req.ip;
    if (activeJobs.size() == 0) {
        db.each('Insert or Replace into Clients(C_IP, S_IP,frame) VALUES(?,null,null)', [pro_ip], (err, row) => {
            if (err) {
                throw err;
            }
        });
        activeclients.set(pro_ip, null);
        res.sendFile(__dirname + '/processor.html');
    }
    else
    {
        console.log(pro_ip);
        var new_ip = activeJobs.peek();
        db.each('Select c_frames frme, frames tfrmes from Jobs where IP = ?', [new_ip], (err, row) => {
            if (err) {
                throw err;
            }
            var frme = currentframe.get(new_ip);
            currentframe.set(new_ip, frme+1);
            activeclients.set(pro_ip,new_ip);
            res.render('ObjDect',{user: "http://"+local_ip+":3000/process", title: new_ip, frame : frme, obj : object });
            console.log(frme);
            if (row.frme + 1 > row.tfrmes) {
                if(activeJobs.peek()==new_ip)
                    completedJobs.enqueue(activeJobs.dequeue());
                db.each('DELETE From Jobs Where IP = ?', [new_ip], (err, row) => {
                    if (err) {
                        throw err;
                    }
                });
                if(new_ip!=null) {
                    db.each('Insert or Replace into Results(IP,frame) VALUES(?,0)', [new_ip], (err, row) => {
                        if (err) {
                            throw err;
                        }
                    });
                }
            } else {
                db.each('Update Jobs SET c_frames = ? where IP = ?', [frme + 1, new_ip], (err, row) => {
                    if (err) {
                        throw err;
                    }
                });
            }
            db.each('Insert or Replace into Clients(C_IP, S_IP,frame) VALUES(?,?,?)', [req.ip,new_ip,frme], (err, row) => {
                if (err) {
                    throw err;
                }
            });
            /*res.sendFile(__dirname + '/uploads/Frames/' + activeJobs.peek() + '/' + activeJobs.peek() + '-' + row.frme + '.jpg');*/
            console.log('Frame: ' + frme);

        });
    }

});

//Upload Video Page
app.get('/upload', function(req, res){
    res.sendFile(__dirname + '/uploadVideo.html');
});

//Redirected to this address once we find the object
app.get('/Found', function(req, res){
    db.each('Select S_IP ip, frame frme from Clients where C_IP=?', [req.ip], (err, row) => {
        if (err) {
            throw err;
        }
        db.each('DELETE From Jobs Where IP = ?', [row.ip], (err, row) => {
            if (err) {
                throw err;
            }
        });
        if(row.ip!=null)
        {
            db.each('Insert or Replace into Results(IP,frame) VALUES(?,?)', [row.ip, row.frme], (err, row) => {
                if (err) {
                    throw err;
                }
            });
        }

        /*db.each('DELETE From Clients Where S_IP = ?', [row.ip], (err, row) => {
            if (err) {
                throw err;
            }
        });*/

        if(activeJobs.peek()==row.ip)completedJobs.enqueue(activeJobs.dequeue());
    });
    db.each('Insert or Replace into Clients(C_IP, S_IP,frame) VALUES(?,null,null)', [req.ip], (err, row) => {
        if (err) {
            throw err;
        }
    });
    activeclients.set(req.ip, null);
    res.sendFile(__dirname + '/midpage2.html');
});


//The upload client keeps hitting this IP until its result is announced
app.get('/waiting', function(req, res){
    var waiting_ip = req.ip;
    if(completedJobs.peek()==waiting_ip && (!activeclients.search(waiting_ip))) {
        db.each('Select frame frme from Results where IP=?', [waiting_ip], (err, row) => {
            if (err) {
                throw err;
            }
            console.log(waiting_ip, row.frme);
            if(row.frme==0){
                res.render('Finallanding', {user: "The requested object is not detected in the Video you uploaded", title:""});
            }
            else {
                res.render('Finallanding', {user: "The requested object was detected in frame "+row.frme+ " of the video you provided", title: "http://"+local_ip+":3000/final"});
            }
            completedJobs.dequeue();
        });
    }
    else {
        res.sendFile(__dirname + '/midpage.html');
    }

});


//Host the final frame that was found
app.get('/final', function(req, res){
    var waiting_ip = req.ip;
    db.each('Select frame frme from Results where IP=?', [waiting_ip], (err, row) => {
        if (err) {
            throw err;
        }
        db.each('DELETE From Results Where IP = ?', [waiting_ip], (err, row) => {
            if (err) {
                throw err;
            }
        });

        res.sendFile(__dirname + '/uploads/Frames/' + waiting_ip + '/' + waiting_ip+ '-' + row.frme + '.jpg');
    });
/*
    rmdir.sync(local_path+'/uploads/Frames/'+waiting_ip);
    rmdir.sync(local_path+'/uploads/ImageUploads/'+ waiting_ip + '.jpg');
    rmdir.sync(local_path+'/uploads/VideoUploads/'+ waiting_ip + '.mp4');*/

});


app.get('/delete', function(req, res){
    waiting_ip=req.ip;
    rmdir.sync(local_path+'/uploads/Frames/'+waiting_ip);
    rmdir.sync(local_path+'/uploads/ImageUploads/'+ waiting_ip + '.jpeg');
    res.sendFile(__dirname + '/index.html');

});


//hosting the application
http.listen(port,local_ip, function(){
    console.log('listening on :' + port);
});
/*db.close((err) => {
    if (err) {
        return console.error(err.message);
    }
    console.log('Close the database connection.');
});*/