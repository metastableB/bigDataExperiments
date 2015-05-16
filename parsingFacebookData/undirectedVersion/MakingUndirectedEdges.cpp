/*
 * Don K Dennis (metastableB)
 * 16 May 2015
 * donkdennis [at] gmail.com
 *
 * Parsing input from the SNAP facebook dataset
 * A small program to create the edge U,V for every edge v,u
 *
 * USAGE : <inpFile >outFile
 * inpFile should have -1 as the last line
 * http://snap.stanford.edu/data/egonets-Facebook.html
 */




#include <iostream>
#include <string>
using namespace std;

int main(int argc , char *argv[]) {
	string s1 , s2;
	while (true) {
		cin >> s1;
		if(!s1.compare("-1")) 
			break;
		cin >> s2;
		cout << s2 <<"\t"<< s1 << "\n";
		cout << s1 <<"\t"<< s2 << "\n";
	}
	return 0;
}