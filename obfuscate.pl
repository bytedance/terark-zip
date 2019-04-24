#!/usr/bin/perl

#use File::BOM; #fuck default no bom lib
#use Regexp::Common;
#use Math::Int64 qw(int64 uint64);

my $cpp = do { local $/; <STDIN> };
my $lastPos = 0;
#$cpp = File::BOM::decode_from_bom($cpp);
#if (substr($cpp,0,1) == chr(0xFEFF) or substr($cpp,0,1) == chr(0xFFFE)) {
#	print STDERR "FUCK BOM perl\n";
#	$cpp = substr($cpp, 3);
#}
#print $cpp;
#$cpp =~ s/$RE{comment}{"C++"}//g; # fuck, it removes //.*\n
$cpp =~ s{//.*$}{}gm;
$cpp =~ s{/\*.*?\*/}{}gms;
#print $cpp;
#exit;

my @alltoken = ( "Hello World!" );
my $lastLineIsContinue = 0;

while ($cpp =~ m{"(?:\\\\|[^\r\n]*?\\")*[^\r\n]*?"|
				 R"\w+\((?:.*?)\)\w+"|
				 '(?:\\'|[^\r\n']+)'|
				# floating number:
			    \b[+\-]?(?:(?:[0-9]*\.[0-9]+)|(?:[0-9]+\.[0-9]*)|[0-9]+)
		   (?:[eE](?:[+\-]?[0-9]+))?
				[fF]?\b|

				^\s*\#[^\r\n]*|
				^[^\r\n]*\\(?=[\r\n])|
				__attribute__\(\(.*?\)\)|
				attribute\(\(.*?\)\)|
				&&|
				\|\||
				\|=|
				==|
				&=|
				~=|
				/=|
				%=|
				\*=|
				\+=|
				-=|
				\^=|
				::|
				\.\.\.|
				>>=|
				<<=|
				>>|
				<<|
				<=|
				>=|
				\(|
				\(|
				\[|
				\]|
				\+\+|
				--|
				\w+
				}gxms) {
	my $thisBegPos = $-[0];
	my $prev = substr($cpp, $lastPos, $thisBegPos - $lastPos);
	if ($lastLineIsContinue) {
		#print "\n-------------------------------\n";
		$cpp =~ m/.*?$/gm;
		#print "+++++++++++++++$&------------\n";
	}
	my $thisEndPos = $+[0];
	my $token = substr($cpp, $thisBegPos, $thisEndPos - $thisBegPos);
	my $isContinue;
   	if (substr($cpp, $thisEndPos-1, 2) =~ /\\[\r\n]/) {
		$isContinue = 1;
	} else {
		$isContinue = 0;
	}
	my $isPrepro;
	if ($token =~ /^\s*#/) {
		$isPrepro = 1;
	} else {
		$isPrepro = 0;
	}
	push @alltoken, $token;

	#print " isContinue = $isContinue, lastLineIsContinue = $lastLineIsContinue\n";

	if ($lastLineIsContinue or $isContinue or $isPrepro) {
		print substr($cpp, $lastPos, $thisEndPos - $lastPos);
	}
	else {
		if (rand(100) < 30) {
			print $prev;
		} else {
			print $prev, "\n";
		}
		if (0 && rand(100) < 35 && $token =~ /[0-9]+|0[Xx][0-9A-Fa-f]+/) {
			my $xx = int64(rand(32768));
			my $yy = $xx + int64($token);
			print "(", int64($yy - $xx), ")";
		}
		else {
			print $token;
		}
		if (rand(100) < 20) {
			print "/* ", $alltoken[int(rand(scalar @alltoken))], " */";
		}
	}
	$lastLineIsContinue = $isContinue;
	$lastPos = $thisEndPos;
}
if ($lastPos < length($cpp)) {
	print substr($cpp, $lastPos);
}
