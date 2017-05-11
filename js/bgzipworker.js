//This is a worker so we can use the synchronous filesystem API
importScripts('rawinflate.js');

self.requestFileSystemSync = self.webkitRequestFileSystemSync ||
                             self.requestFileSystemSync;

var bgzipdatafile;
var tabixfile;

self.addEventListener('message',  function(e) {
	var data = e.data;
	var cmd = data.cmd || "";

	if ( cmd=="setfiles" ) {
		setFiles( data.files );
	} else if ( cmd == "getrange" ) {
		getDataRange( data.range );
	} else if ( cmd == "peek" ) {
		getDataPeek( data.peek );
	} else {
		postMessage( "hello" );
	}
}, false);

function setFiles(files) {
	for(var i=0; i<files.length; i++) {
		var bgz = new BGZipFile(files[i]);
		var bgzheader = bgz.getGzipHeader();
		if ( bgzheader && bgzheader.bgzip ) {
			var bgzreader = getBGZipReader(bgz)
			var tbiheader =  readTabixDataHeader(bgzreader);
			if ( tbiheader ) {
				tabixfile = files[i];
			} else {
				bgzipdatafile = files[i];
			}
		}
	}
	if(bgzipdatafile && tabixfile) {
		postMessage(  {msg:"filesset"} );
	} else {
		postMessage(  {msg:"filesnotset", ff:[bgzipdatafile, tabixfile]} );
	}
}

function getDataPeek(n) {
	n = n || 25;
	var tbi = new BGZipFile(tabixfile);
	var tdr = getBGZipReader(tbi)
	var tbiheader = readTabixDataHeader(tdr);
	var bgz = new BGZipFile(bgzipdatafile);
	var tdr = getBGZipReader(bgz)
	var a ="";
	for(var i=0; i<n; i++) {
		a += tdr.readColumns().join(",") + "\n";
	}
	postMessage( {box: a} );
}

function getDataRange(rawrange) {
	var range = parseRange(rawrange);
	var tbi = new BGZipFile(tabixfile);
	var tdr = getBGZipReader(tbi)
	var tbiheader = readTabixDataHeader(tdr);
	var offset = findTabixOffset(tdr,tbiheader,range[0],range[1]);
	tdr = null; //done with this
	//postMessage( tbiheader );
	postMessage( offset );
	postMessage( range );
	var bgz = new BGZipFile(bgzipdatafile);
	var tdr = getBGZipReader(bgz, offset.offset, offset.voffset)
	var comp = getRowRangeComparer(range, tbiheader);
	var matches =[];
	var passed = 0;
	var cols, stat; 
	while(tdr.hasMore && matches.length<1000 && !passed) {
		cols = tdr.readColumns();
		stat = comp(cols);
		if(stat==0) {
			matches.push(cols.join(","));
		} else if (stat==1) {
			passed=true;
		}
	}
	postMessage( {box: matches.join("\n")} );
}

function parseRange(rawrange) {
	var cp = rawrange.replace(/\s/g,"").split(":");
	if (cp.length==1) {
		return [cp[0]];
	}
	var ss = cp[1].replace(/[^\d-+\xb1kmKMbB]/g,"").toLowerCase()
	var re = /(\d+)([+-\xb1])(\d+)([km]?)b?/
	var parts = re.exec(ss);
	if ( parts ) {
		var start = parseInt(parts[1]);
		var diff = parts[2];
		var end = parseInt(parts[3]);
		var units = parts[4];
		if (diff=="+" || diff=="\xb1") {
			if ( units=="k" ) {
				end = end * 1e3;
			} else if(units=="m") {
				end = end * 1e6;
			}
			start = start - end;
			end = start + 2*end;
		}
		return [cp[0], start, end];
	} else {
		throw "Invalid range format"
	}
}

function getRowRangeComparer(range, header) {
	//0 if in range, -1 if before, 1 if after
	var colSeq = header.colSeq - 1;
	var colStart = header.colStart -1;
	var colEnd = ((header.colEnd) ? header.colEnd : header.colStart)-1;
	return function(row) {
		if ( row[colSeq]==range[0] ) {
			if ( row[colEnd] < range[1] ) {
				return -1
			} else if ( row[colStart] > range[2] ) {
				return 1
			} else {
				return 0
			}
		} else {
			var rangeSeqIndex = header.seqNames.indexOf(range[0]);
			var rowSeqIndex = header.seqNames.indexOf(row[colSeq]);
			if ( rowSeqIndex < rangeSeqIndex ) {
				return -1;
			} else {
				return 1;
			}
		}
	}
}

function getBGZipReader(bgz, fileoffset, virtualoffset) {
	var DataSource = function(f) {
		var _f = f;
		this.next = function() {
			return _f.getBlockAsArrayBuffer().buffer;
		}
	}
	var ds = new DataSource(bgz);
	if (fileoffset) {
		var ab = bgz.getBlockAsArrayBuffer(fileoffset).buffer;
	} else {
		var ab = new ArrayBuffer(0);
	}
	var dr = new DataReader(ab, true, ds);
	if (virtualoffset) {
		dr.skip(virtualoffset);
	}
	return dr;
}

function BGZipFile(file) {
	var index=0;
	var _this = this;
	var _file = file;


	this.getGzipHeader = function(offset) {
		offset = offset || index;
		var blob = _file.slice(offset,offset+500);
		var reader = new FileReaderSync();
		var ab = reader.readAsArrayBuffer(blob);
		var dr = new DataReader(ab, true);

		var gzh = {};
		gzh.id1 = dr.readUint8();
		gzh.id2 = dr.readUint8();
		if (gzh.id1 != 31 || gzh.id2 != 139 ) {
			return null;
		}
		gzh.cm = dr.readUint8();

		gzh.flag = {};
		gzh.flag.raw = dr.readUint8();
		gzh.flag.isText = (gzh.flag.raw & 1);
		gzh.flag.hasHcrc = (gzh.flag.raw & 2);
		gzh.flag.hasExtra = (gzh.flag.raw & 4);
		gzh.flag.hasName = (gzh.flag.raw & 8);
		gzh.flag.hasComment = (gzh.flag.raw & 16);

		gzh.time = dr.readUint32();
		gzh.xfl = dr.readUint8();
		gzh.os = dr.readUint8();

		gzh.extra = []
			if ( gzh.flag.hasExtra ) {
				var extralen = dr.readUint16();
				var extrastart = dr.getIndex();
				gzh.xlen = extralen;
				while (dr.getIndex()<extrastart+extralen) {
					var ex = {}
					ex.si = dr.readString(2);
					ex.slen = dr.readUint16();
					if (ex.si=="BC" && ex.slen==2) {
						//is BGZIP
						ex.content = dr.readUint16();
						gzh.bgzip = {blockSize: ex.content};
					} else {
						ex.content = dr.readString(slen);
						//dr.skip(slen);
					}
					gzh.extra.push(ex);
				}
			}
		if ( gzh.flag.hasName ) {
			gzh.filename = dr.readNullTerminatedString();
		}
		if ( gzh.flag.hasComment ) {
			gzh.comment = dr.readNullTerminatedString();
		}
		if ( gzh.flag.hasHcrc ) {
			gzh.crc16 = dr.readUint16;
		}
		gzh.totalLength = dr.getIndex();
		return gzh;
	}

	this.getBlockAsString = function (offset) {
		index = offset || index || 0;
		if (index > _file.size) {
			return null;
		}
		var gzh = _this.getGzipHeader(index);
		var dataoffset = index+gzh.totalLength;
		var datawidth = gzh.bgzip.blockSize - gzh.xlen-19;
		var blob = _file.slice(dataoffset, dataoffset + datawidth);
		var reader = new FileReaderSync();
		var data = RawDeflate.inflate(reader.readAsBinaryString(blob));

		index += gzh.bgzip.blockSize+1;
		return({header:gzh, data:data, next:index}) 
	}

	this.getBlockAsArrayBuffer = function(offset) {
		index = offset ||index || 0;
		if (index > _file.size) {
			return null;
		}
		var gzh = _this.getGzipHeader(index);
		var dataoffset = index+gzh.totalLength;
		var datawidth = gzh.bgzip.blockSize - gzh.xlen-19;
		var blob = _file.slice(dataoffset, dataoffset + datawidth);
		var reader = new FileReaderSync();
		var data = str2ab(RawDeflate.inflate(reader.readAsBinaryString(blob)));

		index += gzh.bgzip.blockSize+1;
		return({header:gzh, buffer:data, next:index}) 
	}

}

function str2ab(str) {
   var buf = new ArrayBuffer(str.length); 
   var bufView = new Uint8Array(buf);
   for (var i=0, strLen=str.length; i<strLen; i++) {
	 bufView[i] = str.charCodeAt(i);
   }
   return buf;
}

function DataReader(ab, little, source) {
	var index = 0;
	var _this = this;
	var littleEndian = little || false;
	var dv = new DataView(ab);

	this.check = function(size) {
		if (index+size <= dv.byteLength) {
			return true;
		}
		var more;
		if (source && source.next && (more = source.next())) {
			var firstLength = dv.byteLength-index;
			var ab = new ArrayBuffer(firstLength + more.byteLength);
			var newdata = new Uint8Array(more);
			var extended = new Uint8Array(ab);
			for(var i=0; i<firstLength; i++) {
				extended[i] = dv.getUint8(index+i);
			}
			extended.set(newdata, firstLength);
			dv = new DataView(ab);
			index = 0;
			return _this.check(size);
		} else {
			return false;	
		}
	}
	this.advance = function(x) {
		index += x;
	}
	
	this.readUint8 = function () {
		_this.check(1);
		var val =  dv.getUint8(index);
		_this.advance(1);
		return val;
	}
	this.readUint8Array = function(len) {
		var r = [];
		for(var i=0; i<len; i++) {
			r.push(_this.readUint8());
		}
		return r;	
	}
	this.readUint16 = function() {
		_this.check(2);
		var val = dv.getUint16(index, littleEndian);
		_this.advance(2);
		return val;
	}
	this.readUint32 = function() {
		_this.check(4);
		var val = dv.getUint32(index, littleEndian);
		_this.advance(4);
		return val;
	}
	this.readInt32 = function() {
		_this.check(4);
		var val = dv.getInt32(index, littleEndian);
		_this.advance(4);
		return val;
	}

	this.readString = function(len) {
		var str = "";
		for(var i=0; i<len; i++) {
			_this.check(1);
			str += String.fromCharCode(dv.getUint8(index));
			_this.advance(1);
		}
		return str;
	}
	this.readNullTerminatedString = function() {
		var str = "";
		_this.check(1);
		var val = dv.getUint8(index);
		while(val != 0) {
			str += String.fromCharCode(val);
			_this.advance(1);
			_this.check(1);
			val = dv.getUint8(index);
		}
		_this.advance(1);
		return str;
	}
	this.readNullSeparatedArray = function(maxlen) {
		_this.check(1);
		var start = index;
		var vals = [];
		while(index-start<maxlen) {
			vals.push(_this.readNullTerminatedString());
		}
		return vals;
		
	}
	this.readColumns = function() {
		var str = "";
		var vals = [];
		_this.check(1);
		var val = dv.getUint8(index);
		while(val != 10 && val != 13) { //cr or lf
			if (val == 9) { //tab
				vals.push(str);
				str="";
			} else {
				str += String.fromCharCode(val);
			}
			_this.advance(1);
			_this.check(1);
			val = dv.getUint8(index);
		}
		if(str.length) {
			vals.push(str);
			str="";
		}
		_this.advance(1);
		if(_this.check(1) && dv.getUint8(index+1)==10) { //check cr+lf
			_this.advance(1);
		}
		return vals;
	}
	this.hasMore = function(x) {
		_this.check(1);
		return index <= dv.byteLength - (x||1);
	}
	this.getIndex = function() {
		return index;
	}
	this.skip = function(x) {
		_this.check(x);
		_this.advance(x)
	}
}
	
var readTabixDataHeader = function(dr) {
	var tbi = {};
	tbi.header = dr.readString(4);
	if (tbi.header != "TBI\1") {
		return null;
	}
	tbi.nref = dr.readInt32();
	tbi.format = dr.readInt32();
	tbi.colSeq = dr.readInt32()
	tbi.colStart = dr.readInt32()
	tbi.colEnd = dr.readInt32()
	tbi.meta = dr.readString(1);
	dr.skip(3);
	tbi.skip = dr.readInt32();
	var namelength = dr.readInt32();
	tbi.seqNames =  dr.readNullSeparatedArray(namelength);

	return tbi;
}

var findTabixBins = function(start, stop) {
	var k=0;
	var bins=[0];
	for(k=   1+(start>>26); k<=   1+(stop>>26); ++k) bins.push(k);
	for(k=   9+(start>>23); k<=   9+(stop>>23); ++k) bins.push(k);
	for(k=  73+(start>>20); k<=  73+(stop>>20); ++k) bins.push(k);
	for(k= 585+(start>>17); k<= 585+(stop>>17); ++k) bins.push(k);
	for(k=4681+(start>>14); k<=4681+(stop>>14); ++k) bins.push(k);
	return(bins);	
}

var findTabixOffset = function(dr,header,chr,pos) {
	var seqindex = header.seqNames.indexOf(chr);
	if (seqindex < 0 ) {
		throw "Region " + chr + " not found in tabix header";
	}
	var intindex = Math.floor(pos/16384);
	postMessage( {pos:pos,intindex: intindex} )
	var bins, binno, chunks, intervals;
	for( var section=0; section<=seqindex && dr.hasMore(); section++ ) {
		bins = dr.readInt32();
		for(var b=0; b<bins; b++) {
			binno = dr.readUint32();
			chunks = dr.readInt32();
			dr.skip(16*chunks);
		}
		intervals = dr.readInt32();
		if (section != seqindex)  {
			dr.skip(8*intervals);
		} else {
			if (intindex > intervals-1) {
				throw "Invalid interval, need " + indindex + ", found " + intervals;
			}
			//dr.skip(8*intindex);
			var offset, highbits, voffset;
            for(var skip=0; skip<=intindex; skip++) {
                voffset = dr.readUint16();
                offset = dr.readUint32();
                highbits = dr.readUint16();
                postMessage( {offset: offset, voffset: voffset, skip:skip});
            }
			while(voffset==0 && offset==0 && intindex<intervals ) {
				//at start of chr, these offsets are not set till first read is found
				//so advance till first non-zero offset
				intindex++;
				voffset = dr.readUint16();
				offset = dr.readUint32();
				highbits = dr.readUint16();
			}
			if(highbits!=0) {
				throw "Invalid virtual offset, >32bit"
			}
			return {offset: offset, voffset: voffset};
		}
	}

}

