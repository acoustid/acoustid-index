#include <QFile>
#include <QTextStream>
#include <QDebug>
#include <QList>
#include <QSet>
#include <stdint.h>
#include <stdio.h>
#include "store/fs_input_stream.h"
#include "timer.h"

size_t bisectLeft(uint32_t key, uint32_t *data, size_t left, size_t right)
{
	size_t origRight = right;
	while (left < right) {
		size_t middle = left + (right - left) / 2;
		//qDebug() << "Bisecting between" << left << "and" << right << ", the middle is" << middle << "(" << data[middle] << ")";
		if (data[middle] < key) {
			left = middle + 1;
		}
		else {
			right = middle;
		}
	}
	if (left > 0 && data[left] > key) {
		left--;
	}
	//qDebug() << "Finished bisecting at" << left << "or" << right;
	return left;
}

size_t bisectLeft(uint32_t key, uint32_t *data, size_t length)
{
	return bisectLeft(key, data, 0, length);
}

static size_t m_height;
static uint32_t **m_data;
static size_t *m_level_sizes;
static size_t m_indexInterval;

#include <algorithm>

void search(uint32_t key, size_t *first_block, size_t *last_block)
{
	ssize_t level = m_height - 1;
	size_t left = 0, right = m_level_sizes[level];
	while (level >= 0) {
		//qDebug() << "\nSearching at level" << level << "for key" << key << "between" << left << "and" << right;
		//qDebug() << "We have " << m_level_sizes[level] << "keys at this level";
		uint32_t *data = m_data[level]; 
		// locate the the first item that is <= than `key`
		right = std::min(right, m_level_sizes[level]);
		left = std::min(bisectLeft(key, data, left, right), right - 1);
		right = left;
		// locate the the first item that is > than `key`
		while (right < m_level_sizes[level] && data[right] <= key) {
			right++;
		}
		//qDebug() << "Going to the next level between" << left << "(" << data[left] <<")" << "and" << right << "(" << data[right] <<")";
		// update pointers to the next level data
		left *= m_indexInterval;
		right *= m_indexInterval;
		level--;
	}
	*first_block = left / m_indexInterval;
	*last_block = right / m_indexInterval - 1;
}

size_t blockSize = 128;

void searchData(uint32_t key, InputStream *input, size_t firstBlock, size_t lastBlock, QList<uint32_t> *result)
{
	//qDebug() << "Searching for data on key" << key << "(" << firstBlock << "-" << lastBlock << ")";
	for (int block = firstBlock; block <= lastBlock; block++) {
		input->seek(blockSize * block);
		size_t blockKeyCount = input->readInt16();
		//qDebug() << "Keys in block" << block << "=" << blockKeyCount;
		//qDebug() << "First key " << m_data[0][block];
		uint32_t lastKey = m_data[0][block];
		uint32_t lastValue = 0;
		for (int i = 0; i < blockKeyCount; i++) {
			uint32_t keyDelta = i ? input->readVInt32() : 0;
			uint32_t valueDelta = input->readVInt32();
			if (keyDelta == 0) {
				lastValue += valueDelta;
			}
			else {
				lastKey += keyDelta;
				lastValue = valueDelta;
			}
			//qDebug() << "keyDelta" << keyDelta;
			//qDebug() << "valueDelta" << valueDelta;
			//qDebug() << "Got key" << lastKey;
			if (lastKey == key) {
				result->append(lastValue);
				//qDebug() << "Found" << lastKey << "with value" << lastValue;
			}
			if (lastKey > key) {
				return;
			}
		}
	}
}

struct CompareByCount
{
	CompareByCount(const QMap<uint32_t, int> &counts) : m_counts(counts) {}
	bool operator()(uint32_t a, uint32_t b)
	{
		return m_counts[a] > m_counts[b];
	}
	const QMap<uint32_t, int> &m_counts;
};

int main(int argc, char **argv)
{
	InputStream *inputStream = FSInputStream::open("segment0.fii");
	m_indexInterval = inputStream->readVInt32();
	qDebug() << "IndexInterval =" << m_indexInterval;
	m_height = 0;
	m_data = new uint32_t*[16];
	m_level_sizes = new size_t[16];
	while (true) {
		size_t levelKeyCount = inputStream->readVInt32();
		qDebug() << "KeyCount" << m_height << "=" << levelKeyCount;
		uint32_t *keys = new uint32_t[levelKeyCount];
		size_t lastKey = 0;
		for (size_t i = 0; i < levelKeyCount; i++) {
			keys[i] = lastKey + inputStream->readVInt32();
			lastKey = keys[i];
		}
		m_data[m_height] = keys;
		m_level_sizes[m_height] = levelKeyCount;
		m_height++;
		if (levelKeyCount < m_indexInterval) {
			break;
		}
	}
	qDebug() << "Height" << m_height;

	uint32_t fp[] = {
		//1422249929,1423361499,1423369727,1423434238,1440084478,1431818750,1465381214,1465348191,1465275743,1465349503,1465247101,1465251709,1465243513,2002057049,1473574747,1440073051,1440077307,1440084479,1423307247,1431818735,1431826815,1431794015,1431786847,1431795037,1465345917,1465349981,1465341789,1465220957,1473576925,1440079871,1440077311,1440019967,1440084479,1473761791,1465381375,-682102306,-682405506,-682404482,-682400402,-674011330,-707692770,-707758306,-673876706,-707432066,-707399329,-707407523,-732446371,-733490820,-733613700,-733612676,-716834948,-716839076,-682098856,-682107064,-686305320,1465245145,1431690749,1440077311,1440084479,1440084478,1440076286,1431818622,1465381246,1465348447,1465274687,1465349437,1465218365,1465210233,1440046969,1439989593,1423245273,1423296475,1440077787,1440084475,1431695867,1431687675,1431826815,1465315679,1465316733,1465378141,1465251161,1465245017,1465253369,1465253371,1473633787,1473633786,1473639930,1473606138,1465209339,1465348351,1465200895,1431646397,1431777453,1440178605,-573054547,-573180995,-573245729,-574243249,-580928995,-547378627,-545477059,-8608227,-143871219,-142748915,-142621889,-144784593,-145307857,-145295569,-145330417,-178888945,-179026161,-179026149,-179026086,-178829446,-178834054,-178964998,-137030150,-136899078
		//-1780968567,-1780710501,-1788955669,-1788943894,-1254203030,-1321307350,-1317112006,-1318126053,-1308795271,-1275298087,-1283657783,-1285751351,-211823207,-211823191,-211955287,-211824229,-216120373,-232897702,-226528918,-142859222,-143119318,-1241961414,-1258652646,-1258662630,-1262565542,-1799490981,-1799560629,-709045669,-704884101,-679726229,-686480917,-686411525,-685886245,-763478821,-755098439,-755083112,-755111544,-755180088,-739473464,-781418040,-781414055,-802709207,-802774741,-797535958,-797534422,-254361813,-241783233,-208360931,-220418211,-1300452003,-1304459811,-1304526339,-1287929363,-1287654547,-1321209287,-1321319911,-1317091815,-1317081285,-1308825301,-1275266966,-1543710502,-1579164470,-1518677046,-1267023142,-1271184646,-1271303430,-1267128710,-1250415058,-1208210130,-1745077210,-1762116554,-1762058218,-1761927081,-692424491,-692399996,-675622780,-709191804,-721711484,-730232188,-734426380,-1808225307,-1875317393,-1875057298,-1318268630,-1276198086,-1276197030,-1293031557,-1292048391,-1300453927,-1233349384,-1235254168,-1285585816,-1283746439,-1216358887,-1220610549,-1220611558,-1216449922,-1233365010,-1216596754,-1208208210,-1241566018,-1258416962,-1258400354,-1258465330,-1266846130,-1266715122,-1267915250,-1259591906,-1251203778,-1754524614,-1754328006,-1763048422,-1761981349,-1749365543,-1782985320,-1783062888,-1783056744,-1778870584,-1787264008,-1791368855
		//1347739966,1347802894,1347833614,1347833630,1347555118,1347429742,1347428667,1364209931,1371537673,281023753,272725833,272826361,272818153,273714601,1364237705,306220425,306283657,306348173,373461181,1985126829,1985061357,1987167197,2004009725,1953690223,-193797585,-193727939,-193932743,-193956296,-193976552,-169793736,-776849624,-743303895,-743242439,-745536166,-779090470,-774896390,-804240150,-804137750,-801905414,1345580490,1427234251,1464977353,1465042888,1465041884,1456661356,1473569132,1473540477,1465348399,1431761199,1431757102,1431692670,1430541662,1430552398,1430548302,1430662522,1430656362,1430667642,1438990683,1975795979,877412619,341624667,341630331,341683433,341814505,1432214745,1443155144,1980073160,1985380744,1993769980,1989321452,1989190381,2010161757,-145247649,-183061905,-183066065,-183110865,-183209153,-200002755,-187354327,-191482584,-174705368,-174715591,-711652023,-711715381,-778824246,-791411238,-799783686,-803945302,-804207446,1360063642,1393618058,1381096585,1381418376,1381422476,1381429676,1381360044,1381261756,1381454269,1398206959,1408688430,1442079022,1424253726,1348757278,1348744990,1348931386,1348865322,1425417530,1423389978,1423185161,1423180041,1414809353,1410618649,1426586985,1427107307,1431105003,1431096715,1607257483,1607435659,1607373263,1608425965
		-965422978,-952364817,-986246953,-1003080754,1144403678,1142324974,1414823663,1414851167,1431629645,1448451853,1180141324,1180013404,1198912380,-948603939,-982035489,-984201729,-983640721,-975313553,-979507921,-943721201,-944390129,-961167297,-957034385,-969649937,-986377993,-1003081266,-1003080993,-1005175057,-1001054481,-992728451,-975934900,-942276852,-958937332,-963128548,-943852740,-943857284,-2017463811,-2051091969,-2049075730,-2023965202,-2023965330,-2015457978,-2097249210,-1024024562,-1033506786,-1020440466,-1052831497,-1070190130,1144402654,1142298350,1415012079,1423244015,1440016973,1465167693,1448580940,1180144460,1180013436,-967511043,-950570019,-984198689,-984197777,-992608913,-711070417,-711465713,-944258033,-944390129,-961228689,-965422993,-952889105,-986304034,-1003081010,-1005177090,-1000972561,-1001096593,-975934900,-950736116,-967319796,-967342308,-959065284,-940183188,-943721988,-2019569185,-2053127954,-2049141522,-2057524114,-2023908242,-2015984562,-956895154,-956829618,-969584529,-952299281,-985722465,1144402830,1412839166,1414930159,1423212271,1423206989,1473572429,1456848717,1180141324,1182110556,1182135165,-948603939,-948481569,-984201873,-1000876689,-992154321,-711072449,-675818225,-944390129,-961232833,-957034385,-952876817,-986246913,-1003080818,-1005178210,-1005183250,-1001055505,-992728355,-975934516
	};
	size_t fpSize = sizeof(fp) / sizeof(fp[0]);

	std::sort(fp, fp + fpSize);

	BufferedInputStream *dataInputStream = FSInputStream::open("segment0.fid");
	dataInputStream->setBufferSize(blockSize);

	Timer tm;
	size_t first_block, last_block;
	QList<uint32_t> ids;
	tm.start();
	for (int i = 0; i < fpSize; i++) {
		search(fp[i], &first_block, &last_block);
		searchData(fp[i], dataInputStream, first_block, last_block, &ids);
		//qDebug() << fp[i];
		//qDebug() << "first_block =" << first_block;
		//qDebug() << "last_block =" << last_block;
	}
	tm.stop();
	QMap<uint32_t, int> idsStats;
	for (int i = 0; i < ids.size(); i++) {
		uint32_t id = ids.at(i);
		idsStats[id] = idsStats[id] + 1;
	}
	QList<uint32_t> uniqueIds = idsStats.keys();
	std::sort(uniqueIds.begin(), uniqueIds.end(), CompareByCount(idsStats));
	for (int i = 0; i < uniqueIds.size(); i++) {
		uint32_t id = uniqueIds.at(i);
		qDebug() << "Found ID" << id << "with count" << idsStats[id];
	}

	qDebug() << "Duration" << tm.duration() << "ms";
	return 0;
}

