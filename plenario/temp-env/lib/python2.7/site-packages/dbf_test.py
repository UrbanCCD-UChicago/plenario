import codecs
import os
import sys
import unittest
import tempfile
import shutil
import dbf
import datetime
from dbf.api import *
import traceback

py_ver = sys.version_info[:2]

if dbf.version != (0, 94, 3):
    raise ValueError("Wrong version of dbf -- should be %d.%02d.%03d" % dbf.version)
else:
    print "\nTesting dbf version %d.%02d.%03d on %s with Python %s\n" % (
        dbf.version + (sys.platform, sys.version) )
    # 2.5 constructs

try:
    all
except NameError:
    def all(iterable):
        for element in iterable:
            if not element:
                return False
        return True




# Walker in Leaves -- by Scot Noel -- http://www.scienceandfantasyfiction.com/sciencefiction/Walker-in-Leaves/walker-in-leaves.htm

words = """
Soft rains, given time, have rounded the angles of great towers. Generation after generation, wind borne seeds have brought down cities amid the gentle tangle of their roots. All statues of stone have been worn away.
Still one statue, not of stone, holds its lines against the passing years.
Sunlight, fading autumn light, warms the sculpture as best it can, almost penetrating to its dreaming core. The figure is that of a woman, once the fair sex of a species now untroubled and long unseen. Man sleeps the sleep of extinction. This one statue remains. Behind the grace of its ivory brow and gentle, unseeing eyes, the statue dreams.
A susurrus of voices, a flutter of images, and the dream tumbles down through the long morning. Suspended. Floating on the stream that brings from the heart of time the wandering self. Maya  for that is the statue s name-- is buoyed by the sensation, rising within the cage of consciousness, but asleep. She has been this way for months: the unmoving figure of a woman caught in mid stride across the glade. The warmth of sunlight on her face makes her wonder if she will ever wake again.
Even at the end, there was no proper word for what Maya has become. Robot. Cybernetic Organism. Android. These are as appropriate to her condition as calling the stars campfires of the night sky and equally precise. It is enough to know that her motive energies are no longer sun and sustenance, and though Maya was once a living woman, a scientist, now she inhabits a form of ageless attraction. It is a form whose energies are flagging.
With great determination, Maya moves toward wakefulness. Flex a finger. Move a hand. Think of the lemurs, their tongues reaching out in stroke after stroke for the drip of the honeyed thorns. Though there is little time left to save her charges, Maya s only choice is the patience of the trees. On the day her energies return, it is autumn of the year following the morning her sleep began. Maya opens her eyes. The woman, the frozen machine --that which is both-- moves once more.
Two lemur cubs tumbling near the edge of the glade take notice. One rushes forward to touch Maya s knee and laugh. Maya reaches out with an arthritic hand, cold in its sculpted smoothness, but the lemur darts away. Leaves swirl about its retreat, making a crisp sound. The cub displays a playfulness Maya s fevered mind cannot match. The second cub rolls between her moss covered feet, laughing. The lemurs are her charges, and she is failing them. Still, it is good to be awake.
Sugar maples and sumacs shoulder brilliant robes. In the low sun, their orange and purple hues startle the heart. Of course, Maya has no beating organ, no heart. Her life energies are transmitted from deep underground. Nor are the cubs truly lemurs, nor the sugar maples the trees of old. The names have carried for ten million seasons, but the species have changed. Once the lemurs inhabited an island off the southeast coast of a place called Africa. Now they are here, much changed, in the great forests of the northern climes.
The young lemurs seem hale, and it speaks well for their consanguine fellows. But their true fate lies in the story of DNA, of a few strands in the matriarchal line, of a sequence code-named "hope." No doubt a once clever acronym, today Maya s clouded mind holds nothing of the ancient codes. She knows only that a poet once spoke of hope as "the thing with feathers that perches in the soul." Emily Dickinson. A strange name, and so unlike the agnomen of the lemurs. What has become of Giver-of-Corn?
Having no reason to alarm the cubs, Maya moves with her hands high, so that any movement will be down as leaves fall. Though anxious about Giver-of-Corn, she ambles on to finish the mission begun six months earlier. Ahead, the shadow of a mound rises up beneath a great oak. A door awaits. Somewhere below the forest, the engine that gives her life weakens. Held in sway to its faltering beat her mind and body froze, sending her into an abyss of dreams. She has been striding toward that door for half a year, unknowing if she would ever wake again.
Vines lose their toughened grip as the door responds to Maya s approach. Regretfully, a tree root snaps, but the door shudders to a halt before its whine of power can cross the glade. Suddenly, an opening has been made into the earth, and Maya steps lightly on its downward slope. Without breathing, she catches a scent of mold and of deep, uncirculated water. A flutter like the sound of wings echoes from the hollow. Her vision adjusts as she descends. In spots, lights attempt to greet her, but it is a basement she enters, flickering and ancient, where the footfalls of millipedes wear tracks in grime older than the forest above. After a long descent, she steps into water.
How long ago was it that the floor was dry? The exactitude of such time, vast time, escapes her.
Once this place sustained great scholars, scientists. Now sightless fish skip through broken walls, retreating as Maya wades their private corridors, finding with each step that she remembers the labyrinthine path to the heart of power. A heart that flutters like dark wings. And with it, she flutters too. The closer she comes to the vault in which the great engine is housed, the less hopeful she becomes.
The vault housing the engine rests beneath a silvered arch. Its mirrored surface denies age, even as a new generation of snails rise up out of the dark pool, mounting first the dais of pearled stone left by their ancestors, the discarded shells of millions, then higher to where the overhang drips, layered in egg sacs bright as coral.
Maya has no need to set the vault door in motion, to break the dance of the snails. The state of things tells her all she needs to know. There shall be no repairs, no rescue; the engine will die, and she with it. Still, it is impossible not to check. At her touch, a breath of firefly lights coalesces within the patient dampness of the room. They confirm. The heart is simply too tired to go on. Its last reserves wield processes of great weight and care, banking the fires of its blood, dimming the furnace into safe resolve. Perhaps a month or two in cooling, then the last fire kindled by man shall die.
For the figure standing knee deep in water the issues are more immediate. The powers that allow her to live will be the first to fade. It is amazing, even now, that she remains cognizant.
For a moment, Maya stands transfixed by her own reflection. The silvered arch holds it as moonlight does a ghost. She is a sculpted thing with shoulders of white marble. Lips of stone. A child s face. No, the grace of a woman resides in the features, as though eternity can neither deny the sage nor touch the youth. Demeter. The Earth Mother.
Maya smiles at the Greek metaphor. She has never before thought of herself as divine, nor monumental. When the energies of the base are withdrawn entirely, she will become immobile. Once a goddess, then a statue to be worn away by endless time, the crumbling remnant of something the self has ceased to be. Maya trembles at the thought. The last conscious reserve of man will soon fade forever from the halls of time.
As if hewn of irresolute marble, Maya begins to shake; were she still human there would be sobs; there would be tears to moisten her grief and add to the dark waters at her feet.
In time, Maya breaks the spell. She sets aside her grief to work cold fingers over the dim firefly controls, giving what priorities remain to her survival. In response, the great engine promises little, but does what it can.
While life remains, Maya determines to learn what she can of the lemurs, of their progress, and the fate of the matriarchal line. There will be time enough for dreams. Dreams. The one that tumbled down through the long morning comes to her and she pauses to consider it. There was a big table. Indistinct voices gathered around it, but the energy of a family gathering filled the space. The warmth of the room curled about her, perfumed by the smell of cooking. An ancient memory, from a time before the shedding of the flesh. Outside, children laughed. A hand took hers in its own, bringing her to a table filled with colorful dishes and surrounded by relatives and friends. Thanksgiving?
They re calling me home, Maya thinks. If indeed her ancestors could reach across time and into a form not of the flesh, perhaps that was the meaning of the dream. I am the last human consciousness, and I am being called home.
With a flutter, Maya is outside, and the trees all but bare of leaves. Something has happened. Weeks have passed and she struggles to take in her situation. This time she has neither dreamed nor stood immobile, but she has been active without memory.
Her arms cradle a lemur, sheltering the pubescent female against the wind. They sit atop a ridge that separates the valley from the forest to the west, and Walker-in-Leaves has been gone too long. That much Maya remembers. The female lemur sighs. It is a rumbling, mournful noise, and she buries her head tighter against Maya. This is Giver-of-Corn, and Walker is her love.
With her free hand, Maya works at a stiff knitting of pine boughs, the blanket which covers their legs. She pulls it up to better shelter Giver-of-Corn. Beside them, on a shell of bark, a sliver of fish has gone bad from inattention.
They wait through the long afternoon, but Walker does not return. When it is warmest and Giver sleeps, Maya rises in stages, gently separating herself from the lemur. She covers her charge well. Soon it will snow.
There are few memories after reaching the vault, only flashes, and that she has been active in a semi-consciousness state frightens Maya. She stumbles away, shaking, but there is no comfort to seek. She does not know if her diminished abilities endanger the lemurs, and considers locking herself beneath the earth. But the sun is warm, and for the moment every thought is a cloudless sky. Memories descend from the past like a lost tribe wandering for home.
To the east lie once powerful lands and remembered sepulchers. The life of the gods, the pulse of kings, it has all vanished and gone. Maya thinks back to the days of man. There was no disaster at the end. Just time. Civilization did not fail, it succumbed to endless seasons. Each vast stretch of years drawn on by the next saw the conquest of earth and stars, then went on, unheeding, until man dwindled and his monuments frayed.
To the west rise groves of oaks and grassland plains, beyond them, mountains that shrugged off civilization more easily than the rest.
Where is the voyager in those leaves?
A flash of time and Maya finds herself deep in the forests to the west. A lemur call escapes her throat, and suddenly she realizes she is searching for Walker-in-Leaves. The season is the same. Though the air is crisp, the trees are not yet unburdened of their color.
"Walker!" she calls out. "Your love is dying. She mourns your absence."
At the crest of a rise, Maya finds another like herself, but one long devoid of life. This sculpted form startles her at first. It has been almost wholly absorbed into the trunk of a great tree. The knee and calf of one leg escape the surrounding wood, as does a shoulder, the curve of a breast, a mournful face. A single hand reaches out from the tree toward the valley below.
In the distance, Maya sees the remnants of a fallen orbiter. Its power nacelle lies buried deep beneath the river that cushioned its fall. Earth and water, which once heaved at the impact, have worn down impenetrable metals and grown a forest over forgotten technologies.
Had the watcher in the tree come to see the fall, or to stand vigil over the corpse? Maya knows only that she must go on before the hills and the trees conspire to bury her. She moves on, continuing to call for Walker-in-Leaves.
In the night, a coyote finally answers Maya, its frenetic howls awakening responses from many cousins, hunting packs holding court up and down the valley.
Giver-of-Corn holds the spark of her generation. It is not much. A gene here and there, a deep manipulation of the flesh. The consciousness that was man is not easy to engender. Far easier to make an eye than a mind to see. Along a path of endless complication, today Giver-of-Corn mourns the absence of her mate. That Giver may die of such stubborn love before passing on her genes forces Maya deeper into the forest, using the last of her strength to call endlessly into the night.
Maya is dreaming. It s Thanksgiving, but the table is cold. The chairs are empty, and no one answers her call. As she walks from room to room, the lights dim and it begins to rain within the once familiar walls.
When Maya opens her eyes, it is to see Giver-of-Corm sleeping beneath a blanket of pine boughs, the young lemur s bushy tail twitching to the rhythm of sorrowful dreams. Maya is awake once more, but unaware of how much time has passed, or why she decided to return. Her most frightening thought is that she may already have found Walker-in-Leaves, or what the coyotes left behind.
Up from the valley, two older lemurs walk arm in arm, supporting one another along the rise. They bring with them a twig basket and a pouch made of hide. The former holds squash, its hollowed interior brimming with water, the latter a corn mash favored by the tribe. They are not without skills, these lemurs. Nor is language unknown to them. They have known Maya forever and treat her, not as a god, but as a force of nature.
With a few brief howls, by clicks, chatters, and the sweeping gestures of their tails, the lemurs make clear their plea. Their words all but rhyme. Giver-of-Corn will not eat for them. Will she eat for Maya?
Thus has the mission to found a new race come down to this: with her last strength, Maya shall spoon feed a grieving female. The thought strikes her as both funny and sad, while beyond her thoughts, the lemurs continue to chatter.
Scouts have been sent, the elders assure Maya, brave sires skilled in tracking. They hope to find Walker before the winter snows. Their voices stir Giver, and she howls in petty anguish at her benefactors, then disappears beneath the blanket. The elders bow their heads and turn to go, oblivious of Maya s failures.
Days pass upon the ridge in a thickness of clouds. Growing. Advancing. Dimmed by the mountainous billows above, the sun gives way to snow, and Maya watches Giver focus ever more intently on the line to the west. As the lemur s strength fails, her determination to await Walker s return seems to grow stronger still.
Walker-in-Leaves holds a spark of his own. He alone ventures west after the harvest. He has done it before, always returning with a colored stone, a bit of metal, or a flower never before seen by the tribe. It is as if some mad vision compels him, for the journey s end brings a collection of smooth and colored booty to be arranged in a crescent beneath a small monolith Walker himself toiled to raise. Large stones and small, the lemur has broken two fingers of its left hand doing this. To Maya, it seems the ambition of butterflies and falling leaves, of no consequence beyond a motion in the sun. The only importance now is to keep the genes within Giver alive.
Long ago, an ambition rose among the last generation of men, of what had once been men: to cultivate a new consciousness upon the Earth. Maya neither led nor knew the masters of the effort, but she was there when the first prosimians arrived, fresh from their land of orchids and baobabs. Men gathered lemurs and said to them "we shall make you men." Long years followed in the work of the genes, gentling the generations forward. Yet with each passing season, the cultivators grew fewer and their skills less true. So while the men died of age, or boredom, or despair, the lemurs prospered in their youth.
To warm the starving lemur, Maya builds a fire. For this feat the tribe has little skill, nor do they know zero, nor that a lever can move the world. She holds Giver close and pulls the rough blanket of boughs about them both.
All this time, Maya s thoughts remain clear, and the giving of comfort comforts her as well.
The snow begins to cover the monument Walker-in-Leaves has built upon the ridge. As Maya stares on and on into the fire, watching it absorb the snow, watching the snow conquer the cold stones and the grasses already bowed under a cloak of white, she drifts into a flutter of reverie, a weakening of consciousness. The gate to the end is closing, and she shall never know  never know.
"I ll take it easy like, an  stay around de house this winter," her father said. "There s carpenter work for me to do."
Other voices joined in around a table upon which a vast meal had been set. Thanksgiving. At the call of their names, the children rushed in from outside, their laughter quick as sunlight, their jackets smelling of autumn and leaves. Her mother made them wash and bow their heads in prayer. Those already seated joined in.
Grandmother passed the potatoes and called Maya her little kolache, rattling on in a series of endearments and concerns Maya s ear could not follow. Her mother passed on the sense of it and reminded Maya of the Czech for Thank you, Grandma.
It s good to be home, she thinks at first, then: where is the walker in those leaves?
A hand on which two fingers lay curled by the power of an old wound touches Maya. It shakes her, then gently moves her arms so that its owner can pull back the warm pine boughs hiding Giver-of Corn. Eyes first, then smile to tail, Giver opens herself to the returning wanderer. Walker-in-Leaves has returned, and the silence of their embrace brings the whole of the ridge alive in a glitter of sun-bright snow. Maya too comes awake, though this time neither word nor movement prevails entirely upon the fog of sleep.
When the answering howls come to the ridge, those who follow help Maya to stand. She follows them back to the shelter of the valley, and though she stumbles, there is satisfaction in the hurried gait, in the growing pace of the many as they gather to celebrate the return of the one. Songs of rejoicing join the undisciplined and cacophonous barks of youth. Food is brought, from the deep stores, from the caves and their recesses. Someone heats fish over coals they have kept sheltered and going for months. The thought of this ingenuity heartens Maya.
A delicacy of honeyed thorns is offered with great ceremony to Giver-of-Corn, and she tastes at last something beyond the bitterness of loss.
Though Walker-in-Leaves hesitates to leave the side of his love, the others demand stories, persuading him to the center where he begins a cacophonous song of his own.
Maya hopes to see what stones Walker has brought from the west this time, but though she tries to speak, the sounds are forgotten. The engine fades. The last flicker of man s fire is done, and with it the effort of her desires overcome her. She is gone.
Around a table suited for the Queen of queens, a thousand and a thousand sit. Mother to daughter, side-by-side, generation after generation of lemurs share in the feast. Maya is there, hearing the excited voices and the stern warnings to prayer. To her left and her right, each daughter speaks freely. Then the rhythms change, rising along one side to the cadence of Shakespeare and falling along the other to howls the forest first knew.
Unable to contain herself, Maya rises. She pushes on toward the head of a table she cannot see, beginning at last to run. What is the height her charges have reached? How far have they advanced? Lemur faces turn to laugh, their wide eyes joyous and amused. As the generations pass, she sees herself reflected in spectacles, hears the jangle of bracelets and burnished metal, watches matrons laugh behind scarves of silk. Then at last, someone with sculpted hands directs her outside, where the other children are at play in the leaves, now and forever.
THE END""".split()

# data
numbers = [2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97,101,103,107,109,113,127,131,137,139,149,151,157,163,167,173,179,181,191,193,197,199,211,223,227,229,233,239,241,251,257,263,269,271,277,281,283,293,307,311,313,317,331,337,347,349,353,359,367,373,379,383,389,397,401,409,419,421,431,433,439,443,449,457,461,463,467,479,487,491,499,503,509,521,523,541]
floats = []
last = 1
for number in numbers:
    floats.append(float(number ** 2 / last))
    last = number

def permutate(Xs, N):
    if N <= 0:
        yield []
        return
    for x in Xs:
        for sub in permutate(Xs, N-1):
            result = [x]+sub                    # don't allow duplicates
            for item in result:
                if result.count(item) > 1:
                    break
            else:
                yield result

def combinate(Xs, N):
    """Generate combinations of N items from list Xs"""
    if N == 0:
        yield []
        return
    for i in xrange(len(Xs)-N+1):
        for r in combinate(Xs[i+1:], N-1):
            yield [Xs[i]] + r

def index(sequence):
    "returns integers 0 - len(sequence)"
    for i in xrange(len(sequence)):
        yield i    

# tests
def active(rec):
    if is_deleted(rec):
        return DoNotIndex
    return dbf.recno(rec)
def inactive(rec):
    if is_deleted(rec):
        return recno(rec)
    return DoNotIndex
class Test_Char(unittest.TestCase):
    def test_exceptions(self):
        "exceptions"
        self.assertRaises(ValueError, Char, 7)
        self.assertRaises(ValueError, Char, ['nope'])
        self.assertRaises(ValueError, Char, True)
        self.assertRaises(ValueError, Char, False)
        self.assertRaises(ValueError, Char, type)
        self.assertRaises(ValueError, Char, str)
        self.assertRaises(ValueError, Char, None)
    def test_bools_and_none(self):
        "booleans and None"
        empty = Char()
        self.assertFalse(bool(empty))
        one = Char(' ')
        self.assertFalse(bool(one))
        actual = Char('1')
        self.assertTrue(bool(actual))
    def test_equality(self):
        "equality"
        a1 = Char('a')
        a2 = 'a '
        self.assertEqual(a1, a2)
        self.assertEqual(a2, a1)
        a3 = 'a '
        a4 = Char('a ')
        self.assertEqual(a3, a4)
        self.assertEqual(a4, a3)
    def test_inequality(self):
        "inequality"
        a1 = Char('ab ')
        a2 = 'a b'
        self.assertNotEqual(a1, a2)
        self.assertNotEqual(a2, a1)
        a3 = 'ab '
        a4 = Char('a b')
        self.assertNotEqual(a3, a4)
        self.assertNotEqual(a4, a3)
    def test_less_than(self):
        "less-than"
        a1 = Char('a')
        a2 = 'a '
        self.assertFalse(a1 < a2)
        self.assertFalse(a2 < a1)
        a3 = 'a '
        a4 = Char('a ')
        self.assertFalse(a3 < a4)
        self.assertFalse(a4 < a3)
        a5 = 'abcd'
        a6 = 'abce'
        self.assertTrue(a5 < a6)
        self.assertFalse(a6 < a5)
    def test_less_than_equal(self):
        "less-than or equal"
        a1 = Char('a')
        a2 = 'a '
        self.assertTrue(a1 <= a2)
        self.assertTrue(a2 <= a1)
        a3 = 'a '
        a4 = Char('a ')
        self.assertTrue(a3 <= a4)
        self.assertTrue(a4 <= a3)
        a5 = 'abcd'
        a6 = 'abce'
        self.assertTrue(a5 <= a6)
        self.assertFalse(a6 <= a5)
    def test_greater_than(self):
        "greater-than or equal"
        a1 = Char('a')
        a2 = 'a '
        self.assertTrue(a1 >= a2)
        self.assertTrue(a2 >= a1)
        a3 = 'a '
        a4 = Char('a ')
        self.assertTrue(a3 >= a4)
        self.assertTrue(a4 >= a3)
        a5 = 'abcd'
        a6 = 'abce'
        self.assertFalse(a5 >= a6)
        self.assertTrue(a6 >= a5)
    def test_greater_than_equal(self):
        "greater-than"
        a1 = Char('a')
        a2 = 'a '
        self.assertFalse(a1 > a2)
        self.assertFalse(a2 > a1)
        a3 = 'a '
        a4 = Char('a ')
        self.assertFalse(a3 > a4)
        self.assertFalse(a4 > a3)
        a5 = 'abcd'
        a6 = 'abce'
        self.assertFalse(a5 > a6)
        self.assertTrue(a6 > a5)

class Test_Date_Time(unittest.TestCase):
    "Testing Date"
    def test_date_creation(self):
        "Date creation"
        date0 = Date()
        date1 = Date()
        date2 = Date.fromymd('        ')
        date5 = Date.fromordinal(0)
        date6 = Date.today()
        date7 = Date.max
        date8 = Date.min
        self.assertRaises(ValueError, Date.fromymd, '00000')
        self.assertRaises(ValueError, Date.fromymd, '00000000')
        self.assertRaises(ValueError, Date, 0, 0, 0)
    def test_date_compare(self):
        "Date comparisons"
        nodate1 = Date()
        nodate2 = Date()
        date1 = Date.fromordinal(1000)
        date2 = Date.fromordinal(2000)
        date3 = Date.fromordinal(3000)
        self.compareTimes(nodate1, nodate2, date1, date2, date3)

    def test_datetime_creation(self):
        "DateTime creation"
        datetime0 = DateTime()
        datetime1 = DateTime()
        datetime5 = DateTime.fromordinal(0)
        datetime6 = DateTime.today()
        datetime7 = DateTime.max
        datetime8 = DateTime.min
    def test_datetime_compare(self):
        "DateTime comparisons"
        nodatetime1 = DateTime()
        nodatetime2 = DateTime()
        datetime1 = Date.fromordinal(1000)
        datetime2 = Date.fromordinal(20000)
        datetime3 = Date.fromordinal(300000)
        self.compareTimes(nodatetime1, nodatetime2, datetime1, datetime2, datetime3)

    def test_time_creation(self):
        "Time creation"
        time0 = Time()
        time1 = Time()
        time7 = Time.max
        time8 = Time.min
    def test_time_compare(self):
        "Time comparisons"
        notime1 = Time()
        notime2 = Time()
        time1 = Date.fromordinal(1000)
        time2 = Date.fromordinal(2000)
        time3 = Date.fromordinal(3000)
        self.compareTimes(notime1, notime2, time1, time2, time3)
    def test_arithmetic(self):
        "Date, DateTime, & Time Arithmetic"
        one_day = datetime.timedelta(1)
        a_day = Date(1970, 5, 20)
        self.assertEqual(a_day + one_day, Date(1970, 5, 21))
        self.assertEqual(a_day - one_day, Date(1970, 5, 19))
        self.assertEqual(datetime.date(1970, 5, 21) - a_day, one_day)
        a_time = Time(12)
        one_second = datetime.timedelta(0, 1, 0)
        self.assertEqual(a_time + one_second, Time(12, 0, 1))
        self.assertEqual(a_time - one_second, Time(11, 59, 59))
        self.assertEqual(datetime.time(12, 0, 1) - a_time, one_second)
        an_appt = DateTime(2012, 4, 15, 12, 30, 00)
        displacement = datetime.timedelta(1, 60*60*2+60*15)
        self.assertEqual(an_appt + displacement, DateTime(2012, 4, 16, 14, 45, 0))
        self.assertEqual(an_appt - displacement, DateTime(2012, 4, 14, 10, 15, 0))
        self.assertEqual(datetime.datetime(2012, 4, 16, 14, 45, 0) - an_appt, displacement)
    def test_none_compare(self):
        "comparisons to None"
        empty_date = Date()
        empty_time = Time()
        empty_datetime = DateTime()
        self.assertEqual(empty_date, None)
        self.assertEqual(empty_time, None)
        self.assertEqual(empty_datetime, None)
    def test_singletons(self):
        "singletons"
        empty_date = Date()
        empty_time = Time()
        empty_datetime = DateTime()
        self.assertTrue(empty_date is NullDate)
        self.assertTrue(empty_time is NullTime)
        self.assertTrue(empty_datetime is NullDateTime)
    def test_boolean_value(self):
        "boolean evaluation"
        empty_date = Date()
        empty_time = Time()
        empty_datetime = DateTime()
        self.assertEqual(bool(empty_date), False)
        self.assertEqual(bool(empty_time), False)
        self.assertEqual(bool(empty_datetime), False)
        actual_date = Date.today()
        actual_time = Time.now()
        actual_datetime = DateTime.now()
        self.assertEqual(bool(actual_date), True)
        self.assertEqual(bool(actual_time), True)
        self.assertEqual(bool(actual_datetime), True)
    def compareTimes(self, empty1, empty2, uno, dos, tres):
        self.assertEqual(empty1, empty2)
        self.assertEqual(uno < dos, True)
        self.assertEqual(uno <= dos, True)
        self.assertEqual(dos <= dos, True)
        self.assertEqual(dos <= tres, True)
        self.assertEqual(dos < tres, True)
        self.assertEqual(tres <= tres, True)
        self.assertEqual(uno == uno, True)
        self.assertEqual(dos == dos, True)
        self.assertEqual(tres == tres, True)
        self.assertEqual(uno != dos, True)
        self.assertEqual(dos != tres, True)
        self.assertEqual(tres != uno, True)
        self.assertEqual(tres >= tres, True)
        self.assertEqual(tres > dos, True)
        self.assertEqual(dos >= dos, True)
        self.assertEqual(dos >= uno, True)
        self.assertEqual(dos > uno, True)
        self.assertEqual(uno >= uno, True)
        self.assertEqual(uno >= dos, False)
        self.assertEqual(uno >= tres, False)
        self.assertEqual(dos >= tres, False)
        self.assertEqual(tres <= dos, False)
        self.assertEqual(tres <= uno, False)
        self.assertEqual(tres < tres, False)
        self.assertEqual(tres < dos, False)
        self.assertEqual(tres < uno, False)
        self.assertEqual(dos < dos, False)
        self.assertEqual(dos < uno, False)
        self.assertEqual(uno < uno, False)
        self.assertEqual(uno == dos, False)
        self.assertEqual(uno == tres, False)
        self.assertEqual(dos == uno, False)
        self.assertEqual(dos == tres, False)
        self.assertEqual(tres == uno, False)
        self.assertEqual(tres == dos, False)
        self.assertEqual(uno != uno, False)
        self.assertEqual(dos != dos, False)
        self.assertEqual(tres != tres, False)
class Test_Null(unittest.TestCase):
    def test_all(self):
        null = Null = dbf.Null()
        self.assertTrue(null is dbf.Null())

        self.assertTrue(null + 1 is Null)
        self.assertTrue(1 + null is Null)
        null += 4
        self.assertTrue(null is Null)
        value = 5
        value += null
        self.assertTrue(value is Null)

        self.assertTrue(null - 2 is Null)
        self.assertTrue(2 - null is Null)
        null -= 5
        self.assertTrue(null is Null)
        value = 6
        value -= null
        self.assertTrue(value is Null)

        self.assertTrue(null / 0 is Null)
        self.assertTrue(3 / null is Null)
        null /= 6
        self.assertTrue(null is Null)
        value = 7
        value /= null
        self.assertTrue(value is Null)

        self.assertTrue(null * -3 is Null)
        self.assertTrue(4 * null is Null)
        null *= 7
        self.assertTrue(null is Null)
        value = 8
        value *= null
        self.assertTrue(value is Null)

        self.assertTrue(null % 1 is Null)
        self.assertTrue(7 % null is Null)
        null %= 1
        self.assertTrue(null is Null)
        value = 9
        value %= null
        self.assertTrue(value is Null)

        self.assertTrue(null ** 2 is Null)
        self.assertTrue(4 ** null is Null)
        null **= 3
        self.assertTrue(null is Null)
        value = 9
        value **= null
        self.assertTrue(value is Null)

        self.assertTrue(null & 1 is Null)
        self.assertTrue(1 & null is Null)
        null &= 1
        self.assertTrue(null is Null)
        value = 1
        value &= null
        self.assertTrue(value is Null)

        self.assertTrue(null ^ 1 is Null)
        self.assertTrue(1 ^ null is Null)
        null ^= 1
        self.assertTrue(null is Null)
        value = 1
        value ^= null
        self.assertTrue(value is Null)

        self.assertTrue(null | 1 is Null)
        self.assertTrue(1 | null is Null)
        null |= 1
        self.assertTrue(null is Null)
        value = 1
        value |= null 
        self.assertTrue(value is Null)

        self.assertTrue(str(divmod(null, 1)) == '(<null>, <null>)')
        self.assertTrue(str(divmod(1, null)) == '(<null>, <null>)')

        self.assertTrue(null << 1 is Null)
        self.assertTrue(2 << null is Null)
        null <<=3
        self.assertTrue(null is Null)
        value = 9
        value <<= null 
        self.assertTrue(value is Null)

        self.assertTrue(null >> 1 is Null)
        self.assertTrue(2 >> null is Null)
        null >>= 3
        self.assertTrue(null is Null)
        value = 9
        value >>= null 
        self.assertTrue(value is Null)

        self.assertTrue(-null is Null)
        self.assertTrue(+null is Null)
        self.assertTrue(abs(null) is Null)
        self.assertTrue(~null is Null)

        self.assertTrue(null.attr is Null)
        self.assertTrue(null() is Null)
        self.assertTrue(getattr(null, 'fake') is Null)

        self.assertRaises(TypeError, hash, null)


class Test_Logical(unittest.TestCase):
    "Testing Logical"
    def test_unknown(self):
        "Unknown"
        for unk in '', '?', ' ', None, Null, Unknown, Other:
            huh = unknown = Logical(unk)
            self.assertEqual(huh == None, True, "huh is %r from %r, which is not None" % (huh, unk))
            self.assertEqual(huh != None, False, "huh is %r from %r, which is not None" % (huh, unk))
            self.assertEqual(huh != True, True, "huh is %r from %r, which is not None" % (huh, unk))
            self.assertEqual(huh == True, False, "huh is %r from %r, which is not None" % (huh, unk))
            self.assertEqual(huh != False, True, "huh is %r from %r, which is not None" % (huh, unk))
            self.assertEqual(huh == False, False, "huh is %r from %r, which is not None" % (huh, unk))
            if py_ver >= (2, 5):
                self.assertEqual((0, 1, 2)[huh], 2)
    def test_true(self):
        "true"
        for true in 'True', 'yes', 't', 'Y', 7, ['blah']:
            huh = Logical(true)
            self.assertEqual(huh == True, True)
            self.assertEqual(huh != True, False)
            self.assertEqual(huh == False, False, "%r is not True" % true)
            self.assertEqual(huh != False, True)
            self.assertEqual(huh == None, False)
            self.assertEqual(huh != None, True)
            if py_ver >= (2, 5):
                self.assertEqual((0, 1, 2)[huh], 1)
    def test_false(self):
        "false"
        for false in 'false', 'No', 'F', 'n', 0, []:
            huh = Logical(false)
            self.assertEqual(huh != False, False)
            self.assertEqual(huh == False, True)
            self.assertEqual(huh != True, True)
            self.assertEqual(huh == True, False)
            self.assertEqual(huh != None, True)
            self.assertEqual(huh == None, False)
            if py_ver >= (2, 5):
                self.assertEqual((0, 1, 2)[huh], 0)
    def test_singletons(self):
        "singletons"
        heh = Logical(True)
        hah = Logical('Yes')
        ick = Logical(False)
        ack = Logical([])
        unk = Logical('?')
        bla = Logical(None)
        self.assertEquals(heh is hah, True)
        self.assertEquals(ick is ack, True)
        self.assertEquals(unk is bla, True)
    def test_error(self):
        "errors"
        self.assertRaises(ValueError, Logical, 'wrong')
    def test_and(self):
        "and"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)
        self.assertEquals((true & true) is true, True)
        self.assertEquals((true & false) is false, True)
        self.assertEquals((false & true) is false, True)
        self.assertEquals((false & false) is false, True)
        self.assertEquals((true & unknown) is unknown, True)
        self.assertEquals((false & unknown) is false, True)
        self.assertEquals((unknown & true) is unknown, True)
        self.assertEquals((unknown & false) is false, True)
        self.assertEquals((unknown & unknown) is unknown, True)
        self.assertEquals((true & True) is true, True)
        self.assertEquals((true & False) is false, True)
        self.assertEquals((false & True) is false, True)
        self.assertEquals((false & False) is false, True)
        self.assertEquals((true & None) is unknown, True)
        self.assertEquals((false & None) is false, True)
        self.assertEquals((unknown & True) is unknown, True)
        self.assertEquals((unknown & False) is false, True)
        self.assertEquals((unknown & None) is unknown, True)
        self.assertEquals((True & true) is true, True)
        self.assertEquals((True & false) is false, True)
        self.assertEquals((False & true) is false, True)
        self.assertEquals((False & false) is false, True)
        self.assertEquals((True & unknown) is unknown, True)
        self.assertEquals((False & unknown) is false, True)
        self.assertEquals((None & true) is unknown, True)
        self.assertEquals((None & false) is false, True)
        self.assertEquals((None & unknown) is unknown, True)
        self.assertEquals(type(true & 0), int)
        self.assertEquals(true & 0, 0)
        self.assertEquals(type(true & 3), int)
        self.assertEquals(true & 3, 1)
        self.assertEquals(type(false & 0), int)
        self.assertEquals(false & 0, 0)
        self.assertEquals(type(false & 2), int)
        self.assertEquals(false & 2, 0)
        self.assertEquals(type(unknown & 0), int)
        self.assertEquals(unknown & 0, 0)
        self.assertEquals(unknown & 2, unknown)

        t = true
        t &= true
        self.assertEquals(t is true, True)
        t = true
        t &= false
        self.assertEquals(t is false, True)
        f = false
        f &= true
        self.assertEquals(f is false, True)
        f = false
        f &= false
        self.assertEquals(f is false, True)
        t = true
        t &= unknown
        self.assertEquals(t is unknown, True)
        f = false
        f &= unknown
        self.assertEquals(f is false, True)
        u = unknown
        u &= true
        self.assertEquals(u is unknown, True)
        u = unknown
        u &= false
        self.assertEquals(u is false, True)
        u = unknown
        u &= unknown
        self.assertEquals(u is unknown, True)
        t = true
        t &= True
        self.assertEquals(t is true, True)
        t = true
        t &= False
        self.assertEquals(t is false, True)
        f = false
        f &= True
        self.assertEquals(f is false, True)
        f = false
        f &= False
        self.assertEquals(f is false, True)
        t = true
        t &= None
        self.assertEquals(t is unknown, True)
        f = false
        f &= None
        self.assertEquals(f is false, True)
        u = unknown
        u &= True
        self.assertEquals(u is unknown, True)
        u = unknown
        u &= False
        self.assertEquals(u is false, True)
        u = unknown
        u &= None
        self.assertEquals(u is unknown, True)
        t = True
        t &= true
        self.assertEquals(t is true, True)
        t = True
        t &= false
        self.assertEquals(t is false, True)
        f = False
        f &= true
        self.assertEquals(f is false, True)
        f = False
        f &= false
        self.assertEquals(f is false, True)
        t = True
        t &= unknown
        self.assertEquals(t is unknown, True)
        f = False
        f &= unknown
        self.assertEquals(f is false, True)
        u = None
        u &= true
        self.assertEquals(u is unknown, True)
        u = None
        u &= false
        self.assertEquals(u is false, True)
        u = None
        u &= unknown
        self.assertEquals(u is unknown, True)
        t = true
        t &= 0
        self.assertEquals(type(true & 0), int)
        t = true
        t &= 0
        self.assertEquals(true & 0, 0)
        t = true
        t &= 3
        self.assertEquals(type(true & 3), int)
        t = true
        t &= 3
        self.assertEquals(true & 3, 1)
        f = false
        f &= 0
        self.assertEquals(type(false & 0), int)
        f = false
        f &= 0
        self.assertEquals(false & 0, 0)
        f = false
        f &= 2
        self.assertEquals(type(false & 2), int)
        f = false
        f &= 2
        self.assertEquals(false & 2, 0)
        u = unknown
        u &= 0
        self.assertEquals(type(unknown & 0), int)
        u = unknown
        u &= 0
        self.assertEquals(unknown & 0, 0)
        u = unknown
        u &= 2
        self.assertEquals(unknown & 2, unknown)

    def test_or(self):
        "or"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)
        self.assertEquals((true | true) is true, True)
        self.assertEquals((true | false) is true, True)
        self.assertEquals((false | true) is true, True)
        self.assertEquals((false | false) is false, True)
        self.assertEquals((true | unknown) is true, True)
        self.assertEquals((false | unknown) is unknown, True)
        self.assertEquals((unknown | true) is true, True)
        self.assertEquals((unknown | false) is unknown, True)
        self.assertEquals((unknown | unknown) is unknown, True)
        self.assertEquals((true | True) is true, True)
        self.assertEquals((true | False) is true, True)
        self.assertEquals((false | True) is true, True)
        self.assertEquals((false | False) is false, True)
        self.assertEquals((true | None) is true, True)
        self.assertEquals((false | None) is unknown, True)
        self.assertEquals((unknown | True) is true, True)
        self.assertEquals((unknown | False) is unknown, True)
        self.assertEquals((unknown | None) is unknown, True)
        self.assertEquals((True | true) is true, True)
        self.assertEquals((True | false) is true, True)
        self.assertEquals((False | true) is true, True)
        self.assertEquals((False | false) is false, True)
        self.assertEquals((True | unknown) is true, True)
        self.assertEquals((False | unknown) is unknown, True)
        self.assertEquals((None | true) is true, True)
        self.assertEquals((None | false) is unknown, True)
        self.assertEquals((None | unknown) is unknown, True)
        self.assertEquals(type(true | 0), int)
        self.assertEquals(true | 0, 1)
        self.assertEquals(type(true | 2), int)
        self.assertEquals(true | 2, 3)
        self.assertEquals(type(false | 0), int)
        self.assertEquals(false | 0, 0)
        self.assertEquals(type(false | 2), int)
        self.assertEquals(false | 2, 2)
        self.assertEquals(unknown | 0, unknown)
        self.assertEquals(unknown | 2, unknown)

        t = true
        t |= true
        self.assertEquals(t is true, True)
        t = true
        t |= false
        self.assertEquals(t is true, True)
        f = false
        f |= true
        self.assertEquals(f is true, True)
        f = false
        f |= false
        self.assertEquals(f is false, True)
        t = true
        t |= unknown
        self.assertEquals(t is true, True)
        f = false
        f |= unknown
        self.assertEquals(f is unknown, True)
        u = unknown
        u |= true
        self.assertEquals(u is true, True)
        u = unknown
        u |= false
        self.assertEquals(u is unknown, True)
        u = unknown
        u |= unknown
        self.assertEquals(u is unknown, True)
        t = true
        t |= True
        self.assertEquals(t is true, True)
        t = true
        t |= False
        self.assertEquals(t is true, True)
        f = false
        f |= True
        self.assertEquals(f is true, True)
        f = false
        f |= False
        self.assertEquals(f is false, True)
        t = true
        t |= None
        self.assertEquals(t is true, True)
        f = false
        f |= None
        self.assertEquals(f is unknown, True)
        u = unknown
        u |= True
        self.assertEquals(u is true, True)
        u = unknown
        u |= False
        self.assertEquals(u is unknown, True)
        u = unknown
        u |= None
        self.assertEquals(u is unknown, True)
        t = True
        t |= true
        self.assertEquals(t is true, True)
        t = True
        t |= false
        self.assertEquals(t is true, True)
        f = False
        f |= true
        self.assertEquals(f is true, True)
        f = False
        f |= false
        self.assertEquals(f is false, True)
        t = True
        t |= unknown
        self.assertEquals(t is true, True)
        f = False
        f |= unknown
        self.assertEquals(f is unknown, True)
        u = None
        u |= true
        self.assertEquals(u is true, True)
        u = None
        u |= false
        self.assertEquals(u is unknown, True)
        u = None
        u |= unknown
        self.assertEquals(u is unknown, True)
        t = true
        t |= 0
        self.assertEquals(type(t), int)
        t = true
        t |= 0
        self.assertEquals(t, 1)
        t = true
        t |= 2
        self.assertEquals(type(t), int)
        t = true
        t |= 2
        self.assertEquals(t, 3)
        f = false
        f |= 0
        self.assertEquals(type(f), int)
        f = false
        f |= 0
        self.assertEquals(f, 0)
        f = false
        f |= 2
        self.assertEquals(type(f), int)
        f = false
        f |= 2
        self.assertEquals(f, 2)
        u = unknown
        u |= 0
        self.assertEquals(u, unknown)

    def test_xor(self):
        "xor"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)
        self.assertEquals((true ^ true) is false, True)
        self.assertEquals((true ^ false) is true, True)
        self.assertEquals((false ^ true) is true, True)
        self.assertEquals((false ^ false) is false, True)
        self.assertEquals((true ^ unknown) is unknown, True)
        self.assertEquals((false ^ unknown) is unknown, True)
        self.assertEquals((unknown ^ true) is unknown, True)
        self.assertEquals((unknown ^ false) is unknown, True)
        self.assertEquals((unknown ^ unknown) is unknown, True)
        self.assertEquals((true ^ True) is false, True)
        self.assertEquals((true ^ False) is true, True)
        self.assertEquals((false ^ True) is true, True)
        self.assertEquals((false ^ False) is false, True)
        self.assertEquals((true ^ None) is unknown, True)
        self.assertEquals((false ^ None) is unknown, True)
        self.assertEquals((unknown ^ True) is unknown, True)
        self.assertEquals((unknown ^ False) is unknown, True)
        self.assertEquals((unknown ^ None) is unknown, True)
        self.assertEquals((True ^ true) is false, True)
        self.assertEquals((True ^ false) is true, True)
        self.assertEquals((False ^ true) is true, True)
        self.assertEquals((False ^ false) is false, True)
        self.assertEquals((True ^ unknown) is unknown, True)
        self.assertEquals((False ^ unknown) is unknown, True)
        self.assertEquals((None ^ true) is unknown, True)
        self.assertEquals((None ^ false) is unknown, True)
        self.assertEquals((None ^ unknown) is unknown, True)
        self.assertEquals(type(true ^ 2), int)
        self.assertEquals(true ^ 2, 3)
        self.assertEquals(type(true ^ 0), int)
        self.assertEquals(true ^ 0, 1)
        self.assertEquals(type(false ^ 0), int)
        self.assertEquals(false ^ 0, 0)
        self.assertEquals(type(false ^ 2), int)
        self.assertEquals(false ^ 2, 2)
        self.assertEquals(unknown ^ 0, unknown)
        self.assertEquals(unknown ^ 2, unknown)

        t = true
        t ^= true
        self.assertEquals(t is false, True)
        t = true
        t ^= false
        self.assertEquals(t is true, True)
        f = false
        f ^= true
        self.assertEquals(f is true, True)
        f = false
        f ^= false
        self.assertEquals(f is false, True)
        t = true
        t ^= unknown
        self.assertEquals(t is unknown, True)
        f = false
        f ^= unknown
        self.assertEquals(f is unknown, True)
        u = unknown
        u ^= true
        self.assertEquals(u is unknown, True)
        u = unknown
        u ^= false
        self.assertEquals(u is unknown, True)
        u = unknown
        u ^= unknown
        self.assertEquals(u is unknown, True)
        t = true
        t ^= True
        self.assertEquals(t is false, True)
        t = true
        t ^= False
        self.assertEquals(t is true, True)
        f = false
        f ^= True
        self.assertEquals(f is true, True)
        f = false
        f ^= False
        self.assertEquals(f is false, True)
        t = true
        t ^= None
        self.assertEquals(t is unknown, True)
        f = false
        f ^= None
        self.assertEquals(f is unknown, True)
        u = unknown
        u ^= True
        self.assertEquals(u is unknown, True)
        u = unknown
        u ^= False
        self.assertEquals(u is unknown, True)
        u = unknown
        u ^= None
        self.assertEquals(u is unknown, True)
        t = True
        t ^= true
        self.assertEquals(t is false, True)
        t = True
        t ^= false
        self.assertEquals(t is true, True)
        f = False
        f ^= true
        self.assertEquals(f is true, True)
        f = False
        f ^= false
        self.assertEquals(f is false, True)
        t = True
        t ^= unknown
        self.assertEquals(t is unknown, True)
        f = False
        f ^= unknown
        self.assertEquals(f is unknown, True)
        u = None
        u ^= true
        self.assertEquals(u is unknown, True)
        u = None
        u ^= false
        self.assertEquals(u is unknown, True)
        u = None
        u ^= unknown
        self.assertEquals(u is unknown, True)
        t = true
        t ^= 0
        self.assertEquals(type(true ^ 0), int)
        t = true
        t ^= 0
        self.assertEquals(true ^ 0, 1)
        t = true
        t ^= 2
        self.assertEquals(type(true ^ 2), int)
        t = true
        t ^= 2
        self.assertEquals(true ^ 2, 3)
        f = false
        f ^= 0
        self.assertEquals(type(false ^ 0), int)
        f = false
        f ^= 0
        self.assertEquals(false ^ 0, 0)
        f = false
        f ^= 2
        self.assertEquals(type(false ^ 2), int)
        f = false
        f ^= 2
        self.assertEquals(false ^ 2, 2)
        u = unknown
        u ^= 0
        self.assertEquals(unknown ^ 0, unknown)
        u = unknown
        u ^= 2
        self.assertEquals(unknown ^ 2, unknown)

    def test_negation(self):
        "negation"
        true = Logical(True)
        false = Logical(False)
        none = Logical(None)
        self.assertEquals(-true, -1)
        self.assertEquals(-false, 0)
        self.assertEquals(-none, none)
    def test_posation(self):
        "posation"
        true = Logical(True)
        false = Logical(False)
        none = Logical(None)
        self.assertEquals(+true, 1)
        self.assertEquals(+false, 0)
        self.assertEquals(+none, none)
    def test_abs(self):
        "abs()"
        true = Logical(True)
        false = Logical(False)
        none = Logical(None)
        self.assertEquals(abs(true), 1)
        self.assertEquals(abs(false), 0)
        self.assertEquals(abs(none), none)
    def test_invert(self):
        "~ operator"
        true = Logical(True)
        false = Logical(False)
        none = Logical(None)
        self.assertEquals(~true, -2)
        self.assertEquals(~false, -1)
        self.assertEquals(~none, none)

    def test_complex(self):
        "complex"
        true = Logical(True)
        false = Logical(False)
        none = Logical(None)
        self.assertEquals(complex(true), complex(1))
        self.assertEquals(complex(false), complex(0))
        self.assertRaises(ValueError, complex, none)

    def test_int(self):
        "int"
        true = Logical(True)
        false = Logical(False)
        none = Logical(None)
        self.assertEquals(int(true), 1)
        self.assertEquals(int(false), 0)
        self.assertRaises(ValueError, int, none)

    def test_long(self):
        "long"
        true = Logical(True)
        false = Logical(False)
        none = Logical(None)
        self.assertEquals(long(true), 1L)
        self.assertEquals(long(false), 0L)
        self.assertRaises(ValueError, long, none)

    def test_float(self):
        "float"
        true = Logical(True)
        false = Logical(False)
        none = Logical(None)
        self.assertEquals(float(true), 1.0)
        self.assertEquals(float(false), 0.0)
        self.assertRaises(ValueError, float, none)

    def test_oct(self):
        "oct"
        true = Logical(True)
        false = Logical(False)
        none = Logical(None)
        self.assertEquals(oct(true), oct(1))
        self.assertEquals(oct(false), oct(0))
        self.assertRaises(ValueError, oct, none)

    def test_hex(self):
        "hex"
        true = Logical(True)
        false = Logical(False)
        none = Logical(None)
        self.assertEquals(hex(true), hex(1))
        self.assertEquals(hex(false), hex(0))
        self.assertRaises(ValueError, hex, none)

    def test_addition(self):
        "addition"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)
        self.assertEquals(true + true, 2)
        self.assertEquals(true + false, 1)
        self.assertEquals(false + true, 1)
        self.assertEquals(false + false, 0)
        self.assertEquals(true + unknown, unknown)
        self.assertEquals(false + unknown, unknown)
        self.assertEquals(unknown + true, unknown)
        self.assertEquals(unknown + false, unknown)
        self.assertEquals(unknown + unknown, unknown)
        self.assertEquals(true + True, 2)
        self.assertEquals(true + False, 1)
        self.assertEquals(false + True, 1)
        self.assertEquals(false + False, 0)
        self.assertEquals(true + None, unknown)
        self.assertEquals(false + None, unknown)
        self.assertEquals(unknown + True, unknown)
        self.assertEquals(unknown + False, unknown)
        self.assertEquals(unknown + None, unknown)
        self.assertEquals(True + true, 2)
        self.assertEquals(True + false, 1)
        self.assertEquals(False + true, 1)
        self.assertEquals(False + false, 0)
        self.assertEquals(True + unknown, unknown)
        self.assertEquals(False + unknown, unknown)
        self.assertEquals(None + true, unknown)
        self.assertEquals(None + false, unknown)
        self.assertEquals(None + unknown, unknown)

        t = true
        t += true
        self.assertEquals(t, 2)
        t = true
        t += false
        self.assertEquals(t, 1)
        f = false
        f += true
        self.assertEquals(f, 1)
        f = false
        f += false
        self.assertEquals(f, 0)
        t = true
        t += unknown
        self.assertEquals(t, unknown)
        f = false
        f += unknown
        self.assertEquals(f, unknown)
        u = unknown
        u += true
        self.assertEquals(u, unknown)
        u = unknown
        u += false
        self.assertEquals(u, unknown)
        u = unknown
        u += unknown
        self.assertEquals(u, unknown)
        t = true
        t += True
        self.assertEquals(t, 2)
        t = true
        t += False
        self.assertEquals(t, 1)
        f = false
        f += True
        self.assertEquals(f, 1)
        f = false
        f += False
        self.assertEquals(f, 0)
        t = true
        t += None
        self.assertEquals(t, unknown)
        f = false
        f += None
        self.assertEquals(f, unknown)
        u = unknown
        u += True
        self.assertEquals(u, unknown)
        u = unknown
        u += False
        self.assertEquals(u, unknown)
        u = unknown
        u += None
        self.assertEquals(u, unknown)
        t = True
        t += true
        self.assertEquals(t, 2)
        t = True
        t += false
        self.assertEquals(t, 1)
        f = False
        f += true
        self.assertEquals(f, 1)
        f = False
        f += false
        self.assertEquals(f, 0)
        t = True
        t += unknown
        self.assertEquals(t, unknown)
        f = False
        f += unknown
        self.assertEquals(f, unknown)
        u = None
        u += true
        self.assertEquals(u, unknown)
        u = None
        u += false
        self.assertEquals(u, unknown)
        u = None
        u += unknown
        self.assertEquals(u, unknown)

    def test_multiplication(self):
        "multiplication"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)
        self.assertEquals(true * true, 1)
        self.assertEquals(true * false, 0)
        self.assertEquals(false * true, 0)
        self.assertEquals(false * false, 0)
        self.assertEquals(true * unknown, unknown)
        self.assertEquals(false * unknown, 0)
        self.assertEquals(unknown * true, unknown)
        self.assertEquals(unknown * false, 0)
        self.assertEquals(unknown * unknown, unknown)
        self.assertEquals(true * True, 1)
        self.assertEquals(true * False, 0)
        self.assertEquals(false * True, 0)
        self.assertEquals(false * False, 0)
        self.assertEquals(true * None, unknown)
        self.assertEquals(false * None, 0)
        self.assertEquals(unknown * True, unknown)
        self.assertEquals(unknown * False, 0)
        self.assertEquals(unknown * None, unknown)
        self.assertEquals(True * true, 1)
        self.assertEquals(True * false, 0)
        self.assertEquals(False * true, 0)
        self.assertEquals(False * false, 0)
        self.assertEquals(True * unknown, unknown)
        self.assertEquals(False * unknown, 0)
        self.assertEquals(None * true, unknown)
        self.assertEquals(None * false, 0)
        self.assertEquals(None * unknown, unknown)

        t = true
        t *= true
        self.assertEquals(t, 1)
        t = true
        t *= false
        self.assertEquals(t, 0)
        f = false
        f *= true
        self.assertEquals(f, 0)
        f = false
        f *= false
        self.assertEquals(f, 0)
        t = true
        t *= unknown
        self.assertEquals(t, unknown)
        f = false
        f *= unknown
        self.assertEquals(f, 0)
        u = unknown
        u *= true
        self.assertEquals(u, unknown)
        u = unknown
        u *= false
        self.assertEquals(u, 0)
        u = unknown
        u *= unknown
        self.assertEquals(u, unknown)
        t = true
        t *= True
        self.assertEquals(t, 1)
        t = true
        t *= False
        self.assertEquals(t, 0)
        f = false
        f *= True
        self.assertEquals(f, 0)
        f = false
        f *= False
        self.assertEquals(f, 0)
        t = true
        t *= None
        self.assertEquals(t, unknown)
        f = false
        f *= None
        self.assertEquals(f, 0)
        u = unknown
        u *= True
        self.assertEquals(u, unknown)
        u = unknown
        u *= False
        self.assertEquals(u, 0)
        u = unknown
        u *= None
        self.assertEquals(u, unknown)
        t = True
        t *= true
        self.assertEquals(t, 1)
        t = True
        t *= false
        self.assertEquals(t, 0)
        f = False
        f *= true
        self.assertEquals(f, 0)
        f = False
        f *= false
        self.assertEquals(f, 0)
        t = True
        t *= unknown
        self.assertEquals(t, unknown)
        f = False
        f *= unknown
        self.assertEquals(f, 0)
        u = None
        u *= true
        self.assertEquals(u, unknown)
        u = None
        u *= false
        self.assertEquals(u, 0)
        u = None
        u *= unknown
        self.assertEquals(u, unknown)
    def test_subtraction(self):
        "subtraction"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)
        self.assertEquals(true - true, 0)
        self.assertEquals(true - false, 1)
        self.assertEquals(false - true, -1)
        self.assertEquals(false - false, 0)
        self.assertEquals(true - unknown, unknown)
        self.assertEquals(false - unknown, unknown)
        self.assertEquals(unknown - true, unknown)
        self.assertEquals(unknown - false, unknown)
        self.assertEquals(unknown - unknown, unknown)
        self.assertEquals(true - True, 0)
        self.assertEquals(true - False, 1)
        self.assertEquals(false - True, -1)
        self.assertEquals(false - False, 0)
        self.assertEquals(true - None, unknown)
        self.assertEquals(false - None, unknown)
        self.assertEquals(unknown - True, unknown)
        self.assertEquals(unknown - False, unknown)
        self.assertEquals(unknown - None, unknown)
        self.assertEquals(True - true, 0)
        self.assertEquals(True - false, 1)
        self.assertEquals(False - true, -1)
        self.assertEquals(False - false, 0)
        self.assertEquals(True - unknown, unknown)
        self.assertEquals(False - unknown, unknown)
        self.assertEquals(None - true, unknown)
        self.assertEquals(None - false, unknown)
        self.assertEquals(None - unknown, unknown)

        t = true
        t -= true
        self.assertEquals(t, 0)
        t = true
        t -= false
        self.assertEquals(t, 1)
        f = false
        f -= true
        self.assertEquals(f, -1)
        f = false
        f -= false
        self.assertEquals(f, 0)
        t = true
        t -= unknown
        self.assertEquals(t, unknown)
        f = false
        f -= unknown
        self.assertEquals(f, unknown)
        u = unknown
        u -= true
        self.assertEquals(u, unknown)
        u = unknown
        u -= false
        self.assertEquals(u, unknown)
        u = unknown
        u -= unknown
        self.assertEquals(u, unknown)
        t = true
        t -= True
        self.assertEquals(t, 0)
        t = true
        t -= False
        self.assertEquals(t, 1)
        f = false
        f -= True
        self.assertEquals(f, -1)
        f = false
        f -= False
        self.assertEquals(f, 0)
        t = true
        t -= None
        self.assertEquals(t, unknown)
        f = false
        f -= None
        self.assertEquals(f, unknown)
        u = unknown
        u -= True
        self.assertEquals(u, unknown)
        u = unknown
        u -= False
        self.assertEquals(u, unknown)
        u = unknown
        u -= None
        self.assertEquals(u, unknown)
        t = True
        t -= true
        self.assertEquals(t, 0)
        t = True
        t -= false
        self.assertEquals(t, 1)
        f = False
        f -= true
        self.assertEquals(f, -1)
        f = False
        f -= false
        self.assertEquals(f, 0)
        t = True
        t -= unknown
        self.assertEquals(t, unknown)
        f = False
        f -= unknown
        self.assertEquals(f, unknown)
        u = None
        u -= true
        self.assertEquals(u, unknown)
        u = None
        u -= false
        self.assertEquals(u, unknown)
        u = None
        u -= unknown
        self.assertEquals(u, unknown)

    def test_division(self):
        "division"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)
        self.assertEquals(true / true, 1)
        self.assertEquals(true / false, unknown)
        self.assertEquals(false / true, 0)
        self.assertEquals(false / false, unknown)
        self.assertEquals(true / unknown, unknown)
        self.assertEquals(false / unknown, unknown)
        self.assertEquals(unknown / true, unknown)
        self.assertEquals(unknown / false, unknown)
        self.assertEquals(unknown / unknown, unknown)
        self.assertEquals(true / True, 1)
        self.assertEquals(true / False, unknown)
        self.assertEquals(false / True, 0)
        self.assertEquals(false / False, unknown)
        self.assertEquals(true / None, unknown)
        self.assertEquals(false / None, unknown)
        self.assertEquals(unknown / True, unknown)
        self.assertEquals(unknown / False, unknown)
        self.assertEquals(unknown / None, unknown)
        self.assertEquals(True / true, 1)
        self.assertEquals(True / false, unknown)
        self.assertEquals(False / true, 0)
        self.assertEquals(False / false, unknown)
        self.assertEquals(True / unknown, unknown)
        self.assertEquals(False / unknown, unknown)
        self.assertEquals(None / true, unknown)
        self.assertEquals(None / false, unknown)
        self.assertEquals(None / unknown, unknown)

        t = true
        t /= true
        self.assertEquals(t, 1)
        t = true
        t /= false
        self.assertEquals(t, unknown)
        f = false
        f /= true
        self.assertEquals(f, 0)
        f = false
        f /= false
        self.assertEquals(f, unknown)
        t = true
        t /= unknown
        self.assertEquals(t, unknown)
        f = false
        f /= unknown
        self.assertEquals(f, unknown)
        u = unknown
        u /= true
        self.assertEquals(u, unknown)
        u = unknown
        u /= false
        self.assertEquals(u, unknown)
        u = unknown
        u /= unknown
        self.assertEquals(u, unknown)
        t = true
        t /= True
        self.assertEquals(t, 1)
        t = true
        t /= False
        self.assertEquals(t, unknown)
        f = false
        f /= True
        self.assertEquals(f, 0)
        f = false
        f /= False
        self.assertEquals(f, unknown)
        t = true
        t /= None
        self.assertEquals(t, unknown)
        f = false
        f /= None
        self.assertEquals(f, unknown)
        u = unknown
        u /= True
        self.assertEquals(u, unknown)
        u = unknown
        u /= False
        self.assertEquals(u, unknown)
        u = unknown
        u /= None
        self.assertEquals(u, unknown)
        t = True
        t /= true
        self.assertEquals(t, 1)
        t = True
        t /= false
        self.assertEquals(t, unknown)
        f = False
        f /= true
        self.assertEquals(f, 0)
        f = False
        f /= false
        self.assertEquals(f, unknown)
        t = True
        t /= unknown
        self.assertEquals(t, unknown)
        f = False
        f /= unknown
        self.assertEquals(f, unknown)
        u = None
        u /= true
        self.assertEquals(u, unknown)
        u = None
        u /= false
        self.assertEquals(u, unknown)
        u = None
        u /= unknown
        self.assertEquals(u, unknown)


        self.assertEquals(true // true, 1)
        self.assertEquals(true // false, unknown)
        self.assertEquals(false // true, 0)
        self.assertEquals(false // false, unknown)
        self.assertEquals(true // unknown, unknown)
        self.assertEquals(false // unknown, unknown)
        self.assertEquals(unknown // true, unknown)
        self.assertEquals(unknown // false, unknown)
        self.assertEquals(unknown // unknown, unknown)
        self.assertEquals(true // True, 1)
        self.assertEquals(true // False, unknown)
        self.assertEquals(false // True, 0)
        self.assertEquals(false // False, unknown)
        self.assertEquals(true // None, unknown)
        self.assertEquals(false // None, unknown)
        self.assertEquals(unknown // True, unknown)
        self.assertEquals(unknown // False, unknown)
        self.assertEquals(unknown // None, unknown)
        self.assertEquals(True // true, 1)
        self.assertEquals(True // false, unknown)
        self.assertEquals(False // true, 0)
        self.assertEquals(False // false, unknown)
        self.assertEquals(True // unknown, unknown)
        self.assertEquals(False // unknown, unknown)
        self.assertEquals(None // true, unknown)
        self.assertEquals(None // false, unknown)
        self.assertEquals(None // unknown, unknown)

        t = true
        t //= true
        self.assertEquals(t, 1)
        t = true
        t //= false
        self.assertEquals(t, unknown)
        f = false
        f //= true
        self.assertEquals(f, 0)
        f = false
        f //= false
        self.assertEquals(f, unknown)
        t = true
        t //= unknown
        self.assertEquals(t, unknown)
        f = false
        f //= unknown
        self.assertEquals(f, unknown)
        u = unknown
        u //= true
        self.assertEquals(u, unknown)
        u = unknown
        u //= false
        self.assertEquals(u, unknown)
        u = unknown
        u //= unknown
        self.assertEquals(u, unknown)
        t = true
        t //= True
        self.assertEquals(t, 1)
        t = true
        t //= False
        self.assertEquals(t, unknown)
        f = false
        f //= True
        self.assertEquals(f, 0)
        f = false
        f //= False
        self.assertEquals(f, unknown)
        t = true
        t //= None
        self.assertEquals(t, unknown)
        f = false
        f //= None
        self.assertEquals(f, unknown)
        u = unknown
        u //= True
        self.assertEquals(u, unknown)
        u = unknown
        u //= False
        self.assertEquals(u, unknown)
        u = unknown
        u //= None
        self.assertEquals(u, unknown)
        t = True
        t //= true
        self.assertEquals(t, 1)
        t = True
        t //= false
        self.assertEquals(t, unknown)
        f = False
        f //= true
        self.assertEquals(f, 0)
        f = False
        f //= false
        self.assertEquals(f, unknown)
        t = True
        t //= unknown
        self.assertEquals(t, unknown)
        f = False
        f //= unknown
        self.assertEquals(f, unknown)
        u = None
        u //= true
        self.assertEquals(u, unknown)
        u = None
        u //= false
        self.assertEquals(u, unknown)
        u = None
        u //= unknown
        self.assertEquals(u, unknown)
    def test_shift(self):
        "<< and >>"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)

        self.assertEquals(true >> true, 0)
        self.assertEquals(true >> false, 1)
        self.assertEquals(false >> true, 0)
        self.assertEquals(false >> false, 0)
        self.assertEquals(true >> unknown, unknown)
        self.assertEquals(false >> unknown, unknown)
        self.assertEquals(unknown >> true, unknown)
        self.assertEquals(unknown >> false, unknown)
        self.assertEquals(unknown >> unknown, unknown)
        self.assertEquals(true >> True, 0)
        self.assertEquals(true >> False, 1)
        self.assertEquals(false >> True, 0)
        self.assertEquals(false >> False, 0)
        self.assertEquals(true >> None, unknown)
        self.assertEquals(false >> None, unknown)
        self.assertEquals(unknown >> True, unknown)
        self.assertEquals(unknown >> False, unknown)
        self.assertEquals(unknown >> None, unknown)
        self.assertEquals(True >> true, 0)
        self.assertEquals(True >> false, 1)
        self.assertEquals(False >> true, 0)
        self.assertEquals(False >> false, 0)
        self.assertEquals(True >> unknown, unknown)
        self.assertEquals(False >> unknown, unknown)
        self.assertEquals(None >> true, unknown)
        self.assertEquals(None >> false, unknown)
        self.assertEquals(None >> unknown, unknown)

        self.assertEquals(true << true, 2)
        self.assertEquals(true << false, 1)
        self.assertEquals(false << true, 0)
        self.assertEquals(false << false, 0)
        self.assertEquals(true << unknown, unknown)
        self.assertEquals(false << unknown, unknown)
        self.assertEquals(unknown << true, unknown)
        self.assertEquals(unknown << false, unknown)
        self.assertEquals(unknown << unknown, unknown)
        self.assertEquals(true << True, 2)
        self.assertEquals(true << False, 1)
        self.assertEquals(false << True, 0)
        self.assertEquals(false << False, 0)
        self.assertEquals(true << None, unknown)
        self.assertEquals(false << None, unknown)
        self.assertEquals(unknown << True, unknown)
        self.assertEquals(unknown << False, unknown)
        self.assertEquals(unknown << None, unknown)
        self.assertEquals(True << true, 2)
        self.assertEquals(True << false, 1)
        self.assertEquals(False << true, 0)
        self.assertEquals(False << false, 0)
        self.assertEquals(True << unknown, unknown)
        self.assertEquals(False << unknown, unknown)
        self.assertEquals(None << true, unknown)
        self.assertEquals(None << false, unknown)
        self.assertEquals(None << unknown, unknown)

        t = true
        t >>= true
        self.assertEquals(t, 0)
        t = true
        t >>= false
        self.assertEquals(t, 1)
        f = false
        f >>= true
        self.assertEquals(f, 0)
        f = false
        f >>= false
        self.assertEquals(f, 0)
        t = true
        t >>= unknown
        self.assertEquals(t, unknown)
        f = false
        f >>= unknown
        self.assertEquals(f, unknown)
        u = unknown
        u >>= true
        self.assertEquals(u, unknown)
        u = unknown
        u >>= false
        self.assertEquals(u, unknown)
        u = unknown
        u >>= unknown
        self.assertEquals(u, unknown)
        t = true
        t >>= True
        self.assertEquals(t, 0)
        t = true
        t >>= False
        self.assertEquals(t, 1)
        f = false
        f >>= True
        self.assertEquals(f, 0)
        f = false
        f >>= False
        self.assertEquals(f, 0)
        t = true
        t >>= None
        self.assertEquals(t, unknown)
        f = false
        f >>= None
        self.assertEquals(f, unknown)
        u = unknown
        u >>= True
        self.assertEquals(u, unknown)
        u = unknown
        u >>= False
        self.assertEquals(u, unknown)
        u = unknown
        u >>= None
        self.assertEquals(u, unknown)
        t = True
        t >>= true
        self.assertEquals(t, 0)
        t = True
        t >>= false
        self.assertEquals(t, 1)
        f = False
        f >>= true
        self.assertEquals(f, 0)
        f = False
        f >>= false
        self.assertEquals(f, 0)
        t = True
        t >>= unknown
        self.assertEquals(t, unknown)
        f = False
        f >>= unknown
        self.assertEquals(f, unknown)
        u = None
        u >>= true
        self.assertEquals(u, unknown)
        u = None
        u >>= false
        self.assertEquals(u, unknown)
        u = None
        u >>= unknown
        self.assertEquals(u, unknown)

        t = true
        t <<= true
        self.assertEquals(t, 2)
        t = true
        t <<= false
        self.assertEquals(t, 1)
        f = false
        f <<= true
        self.assertEquals(f, 0)
        f = false
        f <<= false
        self.assertEquals(f, 0)
        t = true
        t <<= unknown
        self.assertEquals(t, unknown)
        f = false
        f <<= unknown
        self.assertEquals(f, unknown)
        u = unknown
        u <<= true
        self.assertEquals(u, unknown)
        u = unknown
        u <<= false
        self.assertEquals(u, unknown)
        u = unknown
        u <<= unknown
        self.assertEquals(u, unknown)
        t = true
        t <<= True
        self.assertEquals(t, 2)
        t = true
        t <<= False
        self.assertEquals(t, 1)
        f = false
        f <<= True
        self.assertEquals(f, 0)
        f = false
        f <<= False
        self.assertEquals(f, 0)
        t = true
        t <<= None
        self.assertEquals(t, unknown)
        f = false
        f <<= None
        self.assertEquals(f, unknown)
        u = unknown
        u <<= True
        self.assertEquals(u, unknown)
        u = unknown
        u <<= False
        self.assertEquals(u, unknown)
        u = unknown
        u <<= None
        self.assertEquals(u, unknown)
        t = True
        t <<= true
        self.assertEquals(t, 2)
        t = True
        t <<= false
        self.assertEquals(t, 1)
        f = False
        f <<= true
        self.assertEquals(f, 0)
        f = False
        f <<= false
        self.assertEquals(f, 0)
        t = True
        t <<= unknown
        self.assertEquals(t, unknown)
        f = False
        f <<= unknown
        self.assertEquals(f, unknown)
        u = None
        u <<= true
        self.assertEquals(u, unknown)
        u = None
        u <<= false
        self.assertEquals(u, unknown)
        u = None
        u <<= unknown
        self.assertEquals(u, unknown)

    def test_pow(self):
        "**"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)

        self.assertEquals(true ** true, 1)
        self.assertEquals(true ** false, 1)
        self.assertEquals(false ** true, 0)
        self.assertEquals(false ** false, 1)
        self.assertEquals(true ** unknown, unknown)
        self.assertEquals(false ** unknown, unknown)
        self.assertEquals(unknown ** true, unknown)
        self.assertEquals(unknown ** false, 1)
        self.assertEquals(unknown ** unknown, unknown)
        self.assertEquals(true ** True, 1)
        self.assertEquals(true ** False, 1)
        self.assertEquals(false ** True, 0)
        self.assertEquals(false ** False, 1)
        self.assertEquals(true ** None, unknown)
        self.assertEquals(false ** None, unknown)
        self.assertEquals(unknown ** True, unknown)
        self.assertEquals(unknown ** False, 1)
        self.assertEquals(unknown ** None, unknown)
        self.assertEquals(True ** true, 1)
        self.assertEquals(True ** false, 1)
        self.assertEquals(False ** true, 0)
        self.assertEquals(False ** false, 1)
        self.assertEquals(True ** unknown, unknown)
        self.assertEquals(False ** unknown, unknown)
        self.assertEquals(None ** true, unknown)
        self.assertEquals(None ** false, 1)
        self.assertEquals(None ** unknown, unknown)

        t = true
        t **= true
        self.assertEquals(t, 1)
        t = true
        t **= false
        self.assertEquals(t, 1)
        f = false
        f **= true
        self.assertEquals(f, 0)
        f = false
        f **= false
        self.assertEquals(f, 1)
        t = true
        t **= unknown
        self.assertEquals(t, unknown)
        f = false
        f **= unknown
        self.assertEquals(f, unknown)
        u = unknown
        u **= true
        self.assertEquals(u, unknown)
        u = unknown
        u **= false
        self.assertEquals(u, 1)
        u = unknown
        u **= unknown
        self.assertEquals(u, unknown)
        t = true
        t **= True
        self.assertEquals(t, 1)
        t = true
        t **= False
        self.assertEquals(t, 1)
        f = false
        f **= True
        self.assertEquals(f, 0)
        f = false
        f **= False
        self.assertEquals(f, 1)
        t = true
        t **= None
        self.assertEquals(t, unknown)
        f = false
        f **= None
        self.assertEquals(f, unknown)
        u = unknown
        u **= True
        self.assertEquals(u, unknown)
        u = unknown
        u **= False
        self.assertEquals(u, 1)
        u = unknown
        u **= None
        self.assertEquals(u, unknown)
        t = True
        t **= true
        self.assertEquals(t, 1)
        t = True
        t **= false
        self.assertEquals(t, 1)
        f = False
        f **= true
        self.assertEquals(f, 0)
        f = False
        f **= false
        self.assertEquals(f, 1)
        t = True
        t **= unknown
        self.assertEquals(t, unknown)
        f = False
        f **= unknown
        self.assertEquals(f, unknown)
        u = None
        u **= true
        self.assertEquals(u, unknown)
        u = None
        u **= false
        self.assertEquals(u, 1)
        u = None
        u **= unknown
        self.assertEquals(u, unknown)

    def test_mod(self):
        "%"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)

        self.assertEquals(true % true, 0)
        self.assertEquals(true % false, unknown)
        self.assertEquals(false % true, 0)
        self.assertEquals(false % false, unknown)
        self.assertEquals(true % unknown, unknown)
        self.assertEquals(false % unknown, unknown)
        self.assertEquals(unknown % true, unknown)
        self.assertEquals(unknown % false, unknown)
        self.assertEquals(unknown % unknown, unknown)
        self.assertEquals(true % True, 0)
        self.assertEquals(true % False, unknown)
        self.assertEquals(false % True, 0)
        self.assertEquals(false % False, unknown)
        self.assertEquals(true % None, unknown)
        self.assertEquals(false % None, unknown)
        self.assertEquals(unknown % True, unknown)
        self.assertEquals(unknown % False, unknown)
        self.assertEquals(unknown % None, unknown)
        self.assertEquals(True % true, 0)
        self.assertEquals(True % false, unknown)
        self.assertEquals(False % true, 0)
        self.assertEquals(False % false, unknown)
        self.assertEquals(True % unknown, unknown)
        self.assertEquals(False % unknown, unknown)
        self.assertEquals(None % true, unknown)
        self.assertEquals(None % false, unknown)
        self.assertEquals(None % unknown, unknown)

        t = true
        t %= true
        self.assertEquals(t, 0)
        t = true
        t %= false
        self.assertEquals(t, unknown)
        f = false
        f %= true
        self.assertEquals(f, 0)
        f = false
        f %= false
        self.assertEquals(f, unknown)
        t = true
        t %= unknown
        self.assertEquals(t, unknown)
        f = false
        f %= unknown
        self.assertEquals(f, unknown)
        u = unknown
        u %= true
        self.assertEquals(u, unknown)
        u = unknown
        u %= false
        self.assertEquals(u, unknown)
        u = unknown
        u %= unknown
        self.assertEquals(u, unknown)
        t = true
        t %= True
        self.assertEquals(t, 0)
        t = true
        t %= False
        self.assertEquals(t, unknown)
        f = false
        f %= True
        self.assertEquals(f, 0)
        f = false
        f %= False
        self.assertEquals(f, unknown)
        t = true
        t %= None
        self.assertEquals(t, unknown)
        f = false
        f %= None
        self.assertEquals(f, unknown)
        u = unknown
        u %= True
        self.assertEquals(u, unknown)
        u = unknown
        u %= False
        self.assertEquals(u, unknown)
        u = unknown
        u %= None
        self.assertEquals(u, unknown)
        t = True
        t %= true
        self.assertEquals(t, 0)
        t = True
        t %= false
        self.assertEquals(t, unknown)
        f = False
        f %= true
        self.assertEquals(f, 0)
        f = False
        f %= false
        self.assertEquals(f, unknown)
        t = True
        t %= unknown
        self.assertEquals(t, unknown)
        f = False
        f %= unknown
        self.assertEquals(f, unknown)
        u = None
        u %= true
        self.assertEquals(u, unknown)
        u = None
        u %= false
        self.assertEquals(u, unknown)
        u = None
        u %= unknown
        self.assertEquals(u, unknown)

    def test_divmod(self):
        "divmod()"
        true = Logical(True)
        false = Logical(False)
        unknown = Logical(None)

        self.assertEquals(divmod(true, true), (1, 0))
        self.assertEquals(divmod(true, false), (unknown, unknown))
        self.assertEquals(divmod(false, true), (0, 0))
        self.assertEquals(divmod(false, false), (unknown, unknown))
        self.assertEquals(divmod(true, unknown), (unknown, unknown))
        self.assertEquals(divmod(false, unknown), (unknown, unknown))
        self.assertEquals(divmod(unknown, true), (unknown, unknown))
        self.assertEquals(divmod(unknown, false), (unknown, unknown))
        self.assertEquals(divmod(unknown, unknown), (unknown, unknown))
        self.assertEquals(divmod(true, True), (1, 0))
        self.assertEquals(divmod(true, False), (unknown, unknown))
        self.assertEquals(divmod(false, True), (0, 0))
        self.assertEquals(divmod(false, False), (unknown, unknown))
        self.assertEquals(divmod(true, None), (unknown, unknown))
        self.assertEquals(divmod(false, None), (unknown, unknown))
        self.assertEquals(divmod(unknown, True), (unknown, unknown))
        self.assertEquals(divmod(unknown, False), (unknown, unknown))
        self.assertEquals(divmod(unknown, None), (unknown, unknown))
        self.assertEquals(divmod(True, true), (1, 0))
        self.assertEquals(divmod(True, false), (unknown, unknown))
        self.assertEquals(divmod(False, true), (0, 0))
        self.assertEquals(divmod(False, false), (unknown, unknown))
        self.assertEquals(divmod(True, unknown), (unknown, unknown))
        self.assertEquals(divmod(False, unknown), (unknown, unknown))
        self.assertEquals(divmod(None, true), (unknown, unknown))
        self.assertEquals(divmod(None, false), (unknown, unknown))
        self.assertEquals(divmod(None, unknown), (unknown, unknown))

class Test_Quantum(unittest.TestCase):
    "Testing Quantum"
    def test_exceptions(self):
        "errors"
        self.assertRaises(ValueError, Quantum, 'wrong')

    def test_other(self):
        "Other"
        huh = unknown = Quantum('')
        self.assertEqual(huh is dbf.Other, True)
        self.assertEqual((huh != huh) is unknown, True)
        self.assertEqual((huh != True) is unknown, True)
        self.assertEqual((huh != False) is unknown, True)
        if py_ver >= (2, 5):
            self.assertEqual((0, 1, 2)[huh], 2)

        huh = Quantum('?')
        self.assertEqual(huh is dbf.Other, True)
        self.assertEqual((huh != huh) is unknown, True)
        self.assertEqual((huh != True) is unknown, True)
        self.assertEqual((huh != False) is unknown, True)
        if py_ver >= (2, 5):
            self.assertEqual((0, 1, 2)[huh], 2)

        huh = Quantum(' ')
        self.assertEqual(huh is dbf.Other, True)
        self.assertEqual((huh != huh) is unknown, True)
        self.assertEqual((huh != True) is unknown, True)
        self.assertEqual((huh != False) is unknown, True)
        if py_ver >= (2, 5):
            self.assertEqual((0, 1, 2)[huh], 2)

        huh = Quantum(None)
        self.assertEqual(huh is dbf.Other, True)
        self.assertEqual((huh != huh) is unknown, True)
        self.assertEqual((huh != True) is unknown, True)
        self.assertEqual((huh != False) is unknown, True)
        if py_ver >= (2, 5):
            self.assertEqual((0, 1, 2)[huh], 2)

        huh = Quantum(Null())
        self.assertEqual(huh is dbf.Other, True)
        self.assertEqual((huh != huh) is unknown, True)
        self.assertEqual((huh != True) is unknown, True)
        self.assertEqual((huh != False) is unknown, True)
        if py_ver >= (2, 5):
            self.assertEqual((0, 1, 2)[huh], 2)
            
        huh = Quantum(Other)
        self.assertEqual(huh is dbf.Other, True)
        self.assertEqual((huh != huh) is unknown, True)
        self.assertEqual((huh != True) is unknown, True)
        self.assertEqual((huh != False) is unknown, True)
        if py_ver >= (2, 5):
            self.assertEqual((0, 1, 2)[huh], 2)

        huh = Quantum(Unknown)
        self.assertEqual(huh is dbf.Other, True)
        self.assertEqual((huh != huh) is unknown, True)
        self.assertEqual((huh != True) is unknown, True)
        self.assertEqual((huh != False) is unknown, True)
        if py_ver >= (2, 5):
            self.assertEqual((0, 1, 2)[huh], 2)

    def test_true(self):
        "true"
        huh = Quantum('True')
        unknown = Quantum('?')
        self.assertEqual(huh, True)
        self.assertNotEqual(huh, False)
        self.assertEqual((huh != None) is unknown, True)
        if py_ver >= (2, 5):
            self.assertEqual((0, 1, 2)[huh], 1)

        huh = Quantum('yes')
        unknown = Quantum('?')
        self.assertEqual(huh, True)
        self.assertNotEqual(huh, False)
        self.assertEqual((huh != None) is unknown, True)

        huh = Quantum('t')
        unknown = Quantum('?')
        self.assertEqual(huh, True)
        self.assertNotEqual(huh, False)
        self.assertEqual((huh != None) is unknown, True)

        huh = Quantum('Y')
        unknown = Quantum('?')
        self.assertEqual(huh, True)
        self.assertNotEqual(huh, False)
        self.assertEqual((huh != None) is unknown, True)

        huh = Quantum(7)
        unknown = Quantum('?')
        self.assertEqual(huh, True)
        self.assertNotEqual(huh, False)
        self.assertEqual((huh != None) is unknown, True)

        huh = Quantum(['blah'])
        unknown = Quantum('?')
        self.assertEqual(huh, True)
        self.assertNotEqual(huh, False)
        self.assertEqual((huh != None) is unknown, True)

    def test_false(self):
        "false"
        huh = Quantum('false')
        unknown = Quantum('?')
        self.assertEqual(huh, False)
        self.assertNotEqual(huh, True)
        self.assertEqual((huh != None) is unknown, True)
        if py_ver >= (2, 5):
            self.assertEqual((0, 1, 2)[huh], 0)
            
        huh = Quantum('No')
        unknown = Quantum('?')
        self.assertEqual(huh, False)
        self.assertNotEqual(huh, True)
        self.assertEqual((huh != None) is unknown, True)

        huh = Quantum('F')
        unknown = Quantum('?')
        self.assertEqual(huh, False)
        self.assertNotEqual(huh, True)
        self.assertEqual((huh != None) is unknown, True)

        huh = Quantum('n')
        unknown = Quantum('?')
        self.assertEqual(huh, False)
        self.assertNotEqual(huh, True)
        self.assertEqual((huh != None) is unknown, True)

        huh = Quantum(0)
        unknown = Quantum('?')
        self.assertEqual(huh, False)
        self.assertNotEqual(huh, True)
        self.assertEqual((huh != None) is unknown, True)

        huh = Quantum([])
        unknown = Quantum('?')
        self.assertEqual(huh, False)
        self.assertNotEqual(huh, True)
        self.assertEqual((huh != None) is unknown, True)

    def test_singletons(self):
        "singletons"
        heh = Quantum(True)
        hah = Quantum('Yes')
        ick = Quantum(False)
        ack = Quantum([])
        unk = Quantum('?')
        bla = Quantum(None)
        self.assertEquals(heh is hah, True)
        self.assertEquals(ick is ack, True)
        self.assertEquals(unk is bla, True)

    def test_or(self):
        "or"
        true = Quantum(True)
        false = Quantum(False)
        unknown = Quantum(None)
        self.assertEquals(true + true, true)
        self.assertEquals(true + false, true)
        self.assertEquals(false + true, true)
        self.assertEquals(false + false, false)
        self.assertEquals(true + unknown, true)
        self.assertEquals(false + unknown is unknown, True)
        self.assertEquals(unknown + unknown is unknown, True)
        self.assertEquals(true | true, true)
        self.assertEquals(true | false, true)
        self.assertEquals(false | true, true)
        self.assertEquals(false | false, false)
        self.assertEquals(true | unknown, true)
        self.assertEquals(false | unknown is unknown, True)
        self.assertEquals(unknown | unknown is unknown, True)
        self.assertEquals(true + True, true)
        self.assertEquals(true + False, true)
        self.assertEquals(false + True, true)
        self.assertEquals(false + False, false)
        self.assertEquals(true + None, true)
        self.assertEquals(false + None is unknown, True)
        self.assertEquals(unknown + None is unknown, True)
        self.assertEquals(true | True, true)
        self.assertEquals(true | False, true)
        self.assertEquals(false | True, true)
        self.assertEquals(false | False, false)
        self.assertEquals(true | None, true)
        self.assertEquals(false | None is unknown, True)
        self.assertEquals(unknown | None is unknown, True)
        self.assertEquals(True + true, true)
        self.assertEquals(True + false, true)
        self.assertEquals(False + true, true)
        self.assertEquals(False + false, false)
        self.assertEquals(True + unknown, true)
        self.assertEquals(False + unknown is unknown, True)
        self.assertEquals(None + unknown is unknown, True)
        self.assertEquals(True | true, true)
        self.assertEquals(True | false, true)
        self.assertEquals(False | true, true)
        self.assertEquals(False | false, false)
        self.assertEquals(True | unknown, true)
        self.assertEquals(False | unknown is unknown, True)
        self.assertEquals(None | unknown is unknown, True)
    def test_and(self):
        "and"
        true = Quantum(True)
        false = Quantum(False)
        unknown = Quantum(None)
        self.assertEquals(true * true, true)
        self.assertEquals(true * false, false)
        self.assertEquals(false * true, false)
        self.assertEquals(false * false, false)
        self.assertEquals(true * unknown is unknown, True)
        self.assertEquals(false * unknown, false)
        self.assertEquals(unknown * unknown is unknown, True)
        self.assertEquals(true & true, true)
        self.assertEquals(true & false, false)
        self.assertEquals(false & true, false)
        self.assertEquals(false & false, false)
        self.assertEquals(true & unknown is unknown, True)
        self.assertEquals(false & unknown, false)
        self.assertEquals(unknown & unknown is unknown, True)
        self.assertEquals(true * True, true)
        self.assertEquals(true * False, false)
        self.assertEquals(false * True, false)
        self.assertEquals(false * False, false)
        self.assertEquals(true * None is unknown, True)
        self.assertEquals(false * None, false)
        self.assertEquals(unknown * None is unknown, True)
        self.assertEquals(true & True, true)
        self.assertEquals(true & False, false)
        self.assertEquals(false & True, false)
        self.assertEquals(false & False, false)
        self.assertEquals(true & None is unknown, True)
        self.assertEquals(false & None, false)
        self.assertEquals(unknown & None is unknown, True)
        self.assertEquals(True * true, true)
        self.assertEquals(True * false, false)
        self.assertEquals(False * true, false)
        self.assertEquals(False * false, false)
        self.assertEquals(True * unknown is unknown, True)
        self.assertEquals(False * unknown, false)
        self.assertEquals(None * unknown is unknown, True)
        self.assertEquals(True & true, true)
        self.assertEquals(True & false, false)
        self.assertEquals(False & true, false)
        self.assertEquals(False & false, false)
        self.assertEquals(True & unknown is unknown, True)
        self.assertEquals(False & unknown, false)
        self.assertEquals(None & unknown is unknown, True)
    def test_xor(self):
        "xor"
        true = Quantum(True)
        false = Quantum(False)
        unknown = Quantum(None)
        self.assertEquals(true ^ true, false)
        self.assertEquals(true ^ false, true)
        self.assertEquals(false ^ true, true)
        self.assertEquals(false ^ false, false)
        self.assertEquals(true ^ unknown is unknown, True)
        self.assertEquals(false ^ unknown is unknown, True)
        self.assertEquals(unknown ^ unknown is unknown, True)
        self.assertEquals(true ^ True, false)
        self.assertEquals(true ^ False, true)
        self.assertEquals(false ^ True, true)
        self.assertEquals(false ^ False, false)
        self.assertEquals(true ^ None is unknown, True)
        self.assertEquals(false ^ None is unknown, True)
        self.assertEquals(unknown ^ None is unknown, True)
        self.assertEquals(True ^ true, false)
        self.assertEquals(True ^ false, true)
        self.assertEquals(False ^ true, true)
        self.assertEquals(False ^ false, false)
        self.assertEquals(True ^ unknown is unknown, True)
        self.assertEquals(False ^ unknown is unknown, True)
        self.assertEquals(None ^ unknown is unknown, True)
    def test_implication_material(self):
        "implication, material"
        true = Quantum(True)
        false = Quantum(False)
        unknown = Quantum(None)
        self.assertEquals(true >> true, true)
        self.assertEquals(true >> false, false)
        self.assertEquals(false >> true, true)
        self.assertEquals(false >> false, true)
        self.assertEquals(true >> unknown is unknown, True)
        self.assertEquals(false >> unknown, true)
        self.assertEquals(unknown >> unknown is unknown, True)
        self.assertEquals(true >> True, true)
        self.assertEquals(true >> False, false)
        self.assertEquals(false >> True, true)
        self.assertEquals(false >> False, true)
        self.assertEquals(true >> None is unknown, True)
        self.assertEquals(false >> None, true)
        self.assertEquals(unknown >> None is unknown, True)
        self.assertEquals(True >> true, true)
        self.assertEquals(True >> false, false)
        self.assertEquals(False >> true, true)
        self.assertEquals(False >> false, true)
        self.assertEquals(True >> unknown is unknown, True)
        self.assertEquals(False >> unknown, true)
        self.assertEquals(None >> unknown is unknown, True)
    def test_implication_relevant(self):
        "implication, relevant"
        true = Quantum(True)
        false = Quantum(False)
        unknown = Quantum(None)
        Quantum.set_implication('relevant')
        self.assertEquals(true >> true, true)
        self.assertEquals(true >> false, false)
        self.assertEquals(false >> true is unknown, True)
        self.assertEquals(false >> false is unknown, True)
        self.assertEquals(true >> unknown is unknown, True)
        self.assertEquals(false >> unknown is unknown, True)
        self.assertEquals(unknown >> unknown is unknown, True)
        self.assertEquals(true >> True, true)
        self.assertEquals(true >> False, false)
        self.assertEquals(false >> True is unknown, True)
        self.assertEquals(false >> False is unknown, True)
        self.assertEquals(true >> None is unknown, True)
        self.assertEquals(false >> None is unknown, True)
        self.assertEquals(unknown >> None is unknown, True)
        self.assertEquals(True >> true, true)
        self.assertEquals(True >> false, false)
        self.assertEquals(False >> true is unknown, True)
        self.assertEquals(False >> false is unknown, True)
        self.assertEquals(True >> unknown is unknown, True)
        self.assertEquals(False >> unknown is unknown, True)
        self.assertEquals(None >> unknown is unknown, True)
    def test_nand(self):
        "negative and"
        true = Quantum(True)
        false = Quantum(False)
        unknown = Quantum(None)
        self.assertEquals(true.D(true), false)
        self.assertEquals(true.D(false), true)
        self.assertEquals(false.D(true), true)
        self.assertEquals(false.D(false), true)
        self.assertEquals(true.D(unknown) is unknown, True)
        self.assertEquals(false.D(unknown), true)
        self.assertEquals(unknown.D(unknown) is unknown, True)
        self.assertEquals(true.D(True), false)
        self.assertEquals(true.D(False), true)
        self.assertEquals(false.D(True), true)
        self.assertEquals(false.D(False), true)
        self.assertEquals(true.D(None) is unknown, True)
        self.assertEquals(false.D(None), true)
        self.assertEquals(unknown.D(None) is unknown, True)
    def test_negation(self):
        "negation"
        true = Quantum(True)
        false = Quantum(False)
        none = Quantum(None)
        self.assertEquals(-true, false)
        self.assertEquals(-false, true)
        self.assertEquals(-none is none, True)

class Test_Exceptions(unittest.TestCase):
    def test_bad_field_specs_on_creation(self):
        self.assertRaises(InvalidFieldSpecError, Table, ':blah:', 'age N(3,2)')
        self.assertRaises(InvalidFieldSpecError, Table, ':blah:', 'name C(300)')
        self.assertRaises(InvalidFieldSpecError, Table, ':blah:', 'born L(9)')
        self.assertRaises(InvalidFieldSpecError, Table, ':blah:', 'married D(12)')
        self.assertRaises(InvalidFieldSpecError, Table, ':blah:', 'desc M(1)')

    def test_too_many_fields_on_creation(self):
        fields = []
        for i in range(255):
            fields.append('a%03d C(10)' % i)
        Table(':test:', ';'.join(fields))
        fields.append('a255 C(10)')
        self.assertRaises(DbfError, Table, ':test:', ';'.join(fields))

    def test_adding_too_many_fields(self):
        fields = []
        for i in range(255):
            fields.append('a%03d C(10)' % i)
        table = Table(':test:', ';'.join(fields))
        table.open()
        self.assertRaises(DbfError, table.add_fields, 'a255 C(10)')

    def test_adding_too_many_fields_with_null(self):
        fields = []
        for i in range(254):
            fields.append('a%03d C(10) null' % i)
        table = Table(':test:', ';'.join(fields), dbf_type='vfp')
        table.open()
        self.assertRaises(DbfError, table.add_fields, 'a255 C(10)')

    def test_too_many_records_in_table(self):
        "skipped -- test takes waaaaaaay too long"

    def test_too_many_fields_to_change_to_null(self):
        fields = []
        for i in range(255):
            fields.append('a%03d C(10)' % i)
        table = Table(':test:', ';'.join(fields))
        table.open()
        self.assertRaises(DbfError, table.allow_nulls, 'a001')

    def test_adding_existing_field_to_table(self):
        table = Table(':blah:', 'name C(50)')
        self.assertRaises(DbfError, table.add_fields, 'name C(10)')

    def test_deleting_non_existing_field_from_table(self):
        table = Table(':bleh:', 'name C(25)')
        self.assertRaises(DbfError, table.delete_fields, 'age')

    def test_modify_packed_record(self):
        table = Table(':ummm:', 'name C(3); age N(3,0)')
        table.open()
        for person in (('me', 25), ('you', 35), ('her', 29)):
            table.append(person)
        record = table[1]
        dbf.delete(record)
        table.pack()
        self.assertEqual(('you', 35), record)
        self.assertRaises(DbfError, dbf.write, record, **{'age':33})
    def test_read_only(self):
        table = Table(':ahhh:', 'name C(10)')
        table.open(mode=dbf.READ_ONLY)
        self.assertRaises(DbfError, table.append, dict(name='uh uh!'))
    def test_clipper(self):
        table = Table(os.path.join(tempdir, 'temptable'), 'name C(377); thesis C(20179)', dbf_type='clp')
        self.assertRaises(BadDataError, Table, os.path.join(tempdir, 'temptable'))

class Test_Dbf_Creation(unittest.TestCase):
    "Testing table creation..."
    def test_dbf_memory_tables(self):
        "dbf tables in memory"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = Table(':memory:', fieldlist, dbf_type='db3')
                actualFields = table.structure()
                self.assertEqual(fieldlist, actualFields)
                self.assertTrue(all([type(x) is unicode for x in table.field_names]))
    def test_dbf_disk_tables(self):
        "dbf table on disk"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = Table(os.path.join(tempdir, 'temptable'), ';'.join(fieldlist), dbf_type='db3')
                table = Table(os.path.join(tempdir, 'temptable'), dbf_type='db3')
                actualFields = table.structure()
                self.assertEqual(fieldlist, actualFields)
                table = open(table.filename, 'rb')
                try:
                    last_byte = table.read()[-1]
                finally:
                    table.close()
                self.assertEqual(last_byte, '\x1a')
    def test_clp_memory_tables(self):
        "clp tables in memory"
        fields = ['name C(10977)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = Table(':memory:', fieldlist, dbf_type='clp')
                actualFields = table.structure()
                self.assertEqual(fieldlist, actualFields)
                self.assertTrue(all([type(x) is unicode for x in table.field_names]))
    def test_clp_disk_tables(self):
        "clp table on disk"
        table = Table(os.path.join(tempdir, 'temptable'), 'name C(377); thesis C(20179)', dbf_type='clp')
        self.assertEqual(table.record_length, 20557)
        fields = ['name C(10977)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = Table(os.path.join(tempdir, 'temptable'), ';'.join(fieldlist), dbf_type='clp')
                table = Table(os.path.join(tempdir, 'temptable'), dbf_type='clp')
                actualFields = table.structure()
                self.assertEqual(fieldlist, actualFields)
                table = open(table.filename, 'rb')
                try:
                    last_byte = table.read()[-1]
                finally:
                    table.close()
                self.assertEqual(last_byte, '\x1a')
    def test_fp_memory_tables(self):
        "fp tables in memory"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)',
                  'litres F(11,5)', 'blob G', 'graphic P']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = Table(':memory:', ';'.join(fieldlist), dbf_type='vfp')
                actualFields = table.structure()
                self.assertEqual(fieldlist, actualFields)
    def test_fp_disk_tables(self):
        "fp tables on disk"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)',
                  'litres F(11,5)', 'blob G', 'graphic P']
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = Table(os.path.join(tempdir, 'tempfp'), ';'.join(fieldlist), dbf_type='vfp')
                table = Table(os.path.join(tempdir, 'tempfp'), dbf_type='vfp')
                actualFields = table.structure()
                self.assertEqual(fieldlist, actualFields)
    def test_vfp_memory_tables(self):
        "vfp tables in memory"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)',
                  'weight B', 'litres F(11,5)', 'int I', 'birth T', 'blob G', 'graphic P',
                  'menu C(50) binary', 'graduated L null', 'fired D null', 'cipher C(50) nocptrans null',
                  ]
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = Table(':memory:', ';'.join(fieldlist), dbf_type='vfp')
                actualFields = table.structure()
                fieldlist = [f.replace('nocptrans','binary') for f in fieldlist]
                self.assertEqual(fieldlist, actualFields)
    def test_vfp_disk_tables(self):
        "vfp tables on disk"
        fields = ['name C(25)', 'hiredate D', 'male L', 'wisdom M', 'qty N(3,0)',
                  'weight B', 'litres F(11,5)', 'int I', 'birth T', 'blob G', 'graphic P',
                  'menu C(50) binary', 'graduated L null', 'fired D null', 'cipher C(50) nocptrans null',
                  ]
        for i in range(1, len(fields)+1):
            for fieldlist in combinate(fields, i):
                table = Table(os.path.join(tempdir, 'tempvfp'), ';'.join(fieldlist), dbf_type='vfp')
                table = Table(os.path.join(tempdir, 'tempvfp'), dbf_type='vfp')
                actualFields = table.structure()
                fieldlist = [f.replace('nocptrans','binary') for f in fieldlist]
                self.assertEqual(fieldlist, actualFields)
    def test_codepage(self):
        table = Table(os.path.join(tempdir, 'tempvfp'), 'name C(25); male L; fired D null', dbf_type='vfp')
        self.assertEqual(dbf.default_codepage, 'ascii')
        self.assertEqual(table.codepage, dbf.CodePage('ascii'))
        table = Table(os.path.join(tempdir, 'tempvfp'), 'name C(25); male L; fired D null', dbf_type='vfp', codepage='cp850')
        self.assertEqual(table.codepage, dbf.CodePage('cp850'))
        newtable = table.new('tempvfp2', codepage='cp437')
        self.assertEqual(newtable.codepage, dbf.CodePage('cp437'))
        newtable.open()
        newtable.create_backup()
        newtable.close()
        bckup = Table(os.path.join(tempdir, newtable.backup))
        self.assertEqual(bckup.codepage, newtable.codepage)

class Test_Dbf_Records(unittest.TestCase):
    "Testing records"
    def setUp(self):
        #if not os.path.exists(tempdir):
        #    os.mkdir(tempdir)
        self.dbf_table = Table(
                os.path.join(tempdir, 'dbf_table'),
                'name C(25); paid L; qty N(11,5); orderdate D; desc M',
                dbf_type='db3',
                )
        self.vfp_table = Table(
                os.path.join(tempdir, 'vfp_table'),
                'name C(25); paid L; qty N(11,5); orderdate D; desc M; mass B;' +
                ' weight F(18,3); age I; meeting T; misc G; photo P; price Y',
                dbf_type='vfp',
                )

    def tearDown(self):
        self.dbf_table.close()
        self.vfp_table.close()
        #shutil.rmtree(tempdir)

    def test_slicing(self):
        table = self.dbf_table
        table.open()
        table.append(('myself', True, 5.97, dbf.Date(2012, 5, 21), 'really cool'))
        self.assertEquals(table.first_record['name':'qty'], table[0][:3])
    def test_dbf_adding_records(self):
        "dbf table:  adding records"
        table = self.dbf_table
        table.open()
        namelist = []
        paidlist = []
        qtylist = []
        orderlist = []
        desclist = []
        for i in range(len(floats)):
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            namelist.append(unicode(name))
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(desc)
            table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc})
            record = table[-1]
            last_byte = open(table.filename, 'rb').read()[-1]
            self.assertEqual(last_byte, '\x1a')
            self.assertEqual(record.name.strip(), unicode(name))
            self.assertEqual(record.paid, paid)
            self.assertEqual(record.qty, qty)
            self.assertEqual(record.orderdate, orderdate)
            self.assertEqual(record.desc.strip(), unicode(desc))
        # plus a blank record
        namelist.append('')
        paidlist.append(None)
        qtylist.append(None)
        orderlist.append(None)
        desclist.append('')
        table.append()
        self.assertEqual(len(table), len(floats)+1)
        for field in table.field_names:
            self.assertEqual(1, table.field_names.count(field))
        table.close()
        last_byte = open(table.filename, 'rb').read()[-1]
        self.assertEqual(last_byte, '\x1a')
        table = Table(table.filename, dbf_type='db3')
        table.open()
        self.assertEqual(len(table), len(floats)+1)
        for field in table.field_names:
            self.assertEqual(1, table.field_names.count(field))
        i = 0
        for record in table[:-1]:
            self.assertEqual(dbf.recno(record), i)
            self.assertEqual(table[i].name.strip(), namelist[i])
            self.assertEqual(record.name.strip(), namelist[i])
            self.assertEqual(table[i].paid, paidlist[i])
            self.assertEqual(record.paid, paidlist[i])
            self.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            self.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            self.assertEqual(table[i].orderdate, orderlist[i])
            self.assertEqual(record.orderdate, orderlist[i])
            self.assertEqual(table[i].desc.strip(), desclist[i])
            self.assertEqual(record.desc.strip(), desclist[i])
            i += 1
        record = table[-1]
        self.assertEqual(dbf.recno(record), i)
        self.assertEqual(table[i].name.strip(), namelist[i])
        self.assertEqual(record.name.strip(), namelist[i])
        self.assertEqual(table[i].paid, paidlist[i])
        self.assertEqual(record.paid, paidlist[i])
        self.assertEqual(table[i].qty, qtylist[i])
        self.assertEqual(record.qty, qtylist[i])
        self.assertEqual(table[i].orderdate, orderlist[i])
        self.assertEqual(record.orderdate, orderlist[i])
        self.assertEqual(table[i].desc, desclist[i])
        self.assertEqual(record.desc, desclist[i])
        i += 1
        self.assertEqual(i, len(table))
    def test_vfp_adding_records(self):
        "vfp table:  adding records"
        table = self.vfp_table
        table.open()
        namelist = []
        paidlist = []
        qtylist = []
        orderlist = []
        desclist = []
        masslist = []
        weightlist = []
        agelist = []
        meetlist = []
        misclist = []
        photolist = []
        pricelist = []
        for i in range(len(floats)):
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            mass = floats[i] * floats[i] / 2.0
            weight = floats[i] * 3
            age = numbers[i]
            meeting = datetime.datetime((numbers[i] + 2000), (numbers[i] % 12)+1, (numbers[i] % 28)+1, \
                      (numbers[i] % 24), numbers[i] % 60, (numbers[i] * 3) % 60)
            misc = ' '.join(words[i:i+50:3])
            photo = ' '.join(words[i:i+50:7])
            price = Decimal(round(floats[i] * 2.182737, 4))
            namelist.append(unicode(name))
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(unicode(desc))
            masslist.append(mass)
            weightlist.append(weight)
            agelist.append(age)
            meetlist.append(meeting)
            misclist.append(misc)
            photolist.append(photo)
            table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc, \
                    'mass':mass, 'weight':weight, 'age':age, 'meeting':meeting, 'misc':misc, 'photo':photo})
            record = table[-1]
            self.assertEqual(record.name.strip(), unicode(name))
            self.assertEqual(record.paid, paid)
            self.assertEqual(record.qty, qty)
            self.assertEqual(record.orderdate, orderdate)
            self.assertEqual(record.desc.strip(), unicode(desc))
            self.assertEqual(record.mass, mass)
            self.assertEqual(record.weight, weight)
            self.assertEqual(record.age, age)
            self.assertEqual(record.meeting, meeting)
            self.assertEqual(record.misc, misc)
            self.assertEqual(record.photo, photo)
        # plus a blank record
        namelist.append(' ' * 25)
        paidlist.append(Unknown)
        qtylist.append(None)
        orderlist.append(NullDate)
        desclist.append('')
        masslist.append(0.0)
        weightlist.append(None)
        agelist.append(0)
        meetlist.append(NullDateTime)
        misclist.append('')
        photolist.append('')
        pricelist.append(Decimal('0.0'))
        table.append()
        table.close()
        table = Table(table.filename, dbf_type='vfp')
        table.open()
        self.assertEqual(len(table), len(floats)+1)
        i = 0
        for record in table[:-1]:
            self.assertEqual(dbf.recno(record), i)
            self.assertEqual(table[i].name.strip(), namelist[i])
            self.assertEqual(record.name.strip(), namelist[i])
            self.assertEqual(table[i].paid, paidlist[i])
            self.assertEqual(record.paid, paidlist[i])
            self.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            self.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            self.assertEqual(table[i].orderdate, orderlist[i])
            self.assertEqual(record.orderdate, orderlist[i])
            self.assertEqual(table[i].desc.strip(), desclist[i])
            self.assertEqual(record.desc.strip(), desclist[i])
            self.assertEqual(record.mass, masslist[i])
            self.assertEqual(table[i].mass, masslist[i])
            self.assertEqual(record.weight, weightlist[i])
            self.assertEqual(table[i].weight, weightlist[i])
            self.assertEqual(record.age, agelist[i])
            self.assertEqual(table[i].age, agelist[i])
            self.assertEqual(record.meeting, meetlist[i])
            self.assertEqual(table[i].meeting, meetlist[i])
            self.assertEqual(record.misc, misclist[i])
            self.assertEqual(table[i].misc, misclist[i])
            self.assertEqual(record.photo, photolist[i])
            self.assertEqual(table[i].photo, photolist[i])
            i += 1
        record = table[-1]
        self.assertEqual(dbf.recno(record), i)
        self.assertEqual(table[i].name, namelist[i])
        self.assertEqual(record.name, namelist[i])
        self.assertEqual(table[i].paid is None, True)
        self.assertEqual(record.paid is None, True)
        self.assertEqual(table[i].qty, None)
        self.assertEqual(record.qty, None)
        self.assertEqual(table[i].orderdate, orderlist[i])
        self.assertEqual(record.orderdate, orderlist[i])
        self.assertEqual(table[i].desc, desclist[i])
        self.assertEqual(record.desc, desclist[i])
        self.assertEqual(record.mass, masslist[i])
        self.assertEqual(table[i].mass, masslist[i])
        self.assertEqual(record.weight, weightlist[i])
        self.assertEqual(table[i].weight, weightlist[i])
        self.assertEqual(record.age, agelist[i])
        self.assertEqual(table[i].age, agelist[i])
        self.assertEqual(record.meeting, meetlist[i])
        self.assertEqual(table[i].meeting, meetlist[i])
        self.assertEqual(record.misc, misclist[i])
        self.assertEqual(table[i].misc, misclist[i])
        self.assertEqual(record.photo, photolist[i])
        self.assertEqual(table[i].photo, photolist[i])
        i += 1

    def test_char_memo_return_type(self):
        "check character fields return type"
        table = Table(':memory:', 'text C(50); memo M', codepage='cp1252', dbf_type='vfp')
        table.open()
        table.append(('another one bites the dust', "and another one's gone, and another one's gone..."))
        table.append()
        for record in table:
            self.assertTrue(type(record.text) is unicode)
            self.assertTrue(type(record.memo) is unicode)
        
        table = Table(':memory:', 'text C(50); memo M', codepage='cp1252', dbf_type='vfp',
            default_data_types=dict(C=Char, M=Char))
        table.open()
        table.append(('another one bites the dust', "and another one's gone, and another one's gone..."))
        table.append()
        for record in table:
            self.assertTrue(type(record.text) is Char)
            self.assertTrue(type(record.memo) is Char)

        table = Table(':memory:', 'text C(50); memo M', codepage='cp1252', dbf_type='vfp',
            default_data_types=dict(C=(Char, NoneType), M=(Char, NoneType)))
        table.open()
        table.append(('another one bites the dust', "and another one's gone, and another one's gone..."))
        table.append()
        record = table[0]
        self.assertTrue(type(record.text) is Char)
        self.assertTrue(type(record.memo) is Char)
        record = table[1]
        self.assertTrue(type(record.text) is NoneType)
        self.assertTrue(type(record.memo) is NoneType)
    def test_empty_is_none(self):
        "empty and None values"
        table = Table(':memory:', 'name C(20); born L; married D; appt T; wisdom M', dbf_type='vfp')
        table.open()
        table.append()
        record = table[-1]
        self.assertTrue(record.born is None)
        self.assertTrue(record.married is None)
        self.assertTrue(record.appt is None)
        self.assertEqual(record.wisdom, '')
        appt = DateTime.now()
        dbf.write(
                record,
                born = True,
                married = Date(1992, 6, 27),
                appt = appt,
                wisdom = 'Choose Python',
                )
        self.assertTrue(record.born)
        self.assertEqual(record.married, Date(1992, 6, 27))
        self.assertEqual(record.appt, appt)
        self.assertEqual(record.wisdom, 'Choose Python')
        dbf.write(
                record,
                born = Unknown,
                married = NullDate,
                appt = NullDateTime,
                wisdom = '',
                )
        self.assertTrue(record.born is None)
        self.assertTrue(record.married is None)
        self.assertTrue(record.appt is None)
        self.assertEqual(record.wisdom, '')
    def test_custom_data_type(self):
        "custom data types"
        table = Table(
            filename=':memory:',
            field_specs='name C(20); born L; married D; appt T; wisdom M',
            field_data_types=dict(name=Char, born=Logical, married=Date, appt=DateTime, wisdom=Char,),
            dbf_type='vfp'
            )
        table.open()
        table.append()
        record = table[-1]
        self.assertTrue(type(record.name) is Char, "record.name is %r, not Char" % type(record.name))
        self.assertTrue(type(record.born) is Logical, "record.born is %r, not Logical" % type(record.born))
        self.assertTrue(type(record.married) is Date, "record.married is %r, not Date" % type(record.married))
        self.assertTrue(type(record.appt) is DateTime, "record.appt is %r, not DateTime" % type(record.appt))
        self.assertTrue(type(record.wisdom) is Char, "record.wisdom is %r, not Char" % type(record.wisdom))
        self.assertEqual(record.name, ' ' * 20)
        self.assertTrue(record.born is Unknown, "record.born is %r, not Unknown" % record.born)
        self.assertTrue(record.married is NullDate, "record.married is %r, not NullDate" % record.married)
        self.assertEqual(record.married, None)
        self.assertTrue(record.appt is NullDateTime, "record.appt is %r, not NullDateTime" % record.appt)
        self.assertEqual(record.appt, None)
        appt = DateTime.now()
        dbf.write(
                record,
                name = 'Ethan               ',
                born = True,
                married = Date(1992, 6, 27),
                appt = appt,
                wisdom = 'Choose Python',
                )
        self.assertEqual(type(record.name), Char, "record.wisdom is %r, but should be Char" % record.wisdom)
        self.assertTrue(record.born is Truth)
        self.assertEqual(record.married, Date(1992, 6, 27))
        self.assertEqual(record.appt, appt)
        self.assertEqual(type(record.wisdom), Char, "record.wisdom is %r, but should be Char" % record.wisdom)
        self.assertEqual(record.wisdom, 'Choose Python')
        dbf.write(record, born=Falsth)
        self.assertEqual(record.born, False)
        dbf.write(record, born=None, married=None, appt=None, wisdom=None)
        self.assertTrue(record.born is Unknown)
        self.assertTrue(record.married is NullDate)
        self.assertTrue(record.appt is NullDateTime)
        self.assertTrue(type(record.wisdom) is Char, "record.wisdom is %r, but should be Char" % type(record.wisdom))
    def test_datatypes_param(self):
        "field_types with normal data type but None on empty"
        table = Table(
            filename=':memory:',
            field_specs='name C(20); born L; married D; wisdom M',
            field_data_types=dict(name=(str, NoneType), born=(bool, bool)),
            dbf_type='db3'
            )
        table.open()
        table.append()
        record = table[-1]
        self.assertTrue(type(record.name) is type(None), "record.name is %r, not None" % type(record.name))
        self.assertTrue(type(record.born) is bool, "record.born is %r, not bool" % type(record.born))
        self.assertTrue(record.name is None)
        self.assertTrue(record.born is False, "record.born is %r, not False" % record.born)
        dbf.write(record, name='Ethan               ', born=True)
        self.assertEqual(type(record.name), str, "record.name is %r, but should be Char" % record.wisdom)
        self.assertTrue(record.born is True)
        dbf.write(record, born=False)
        self.assertEqual(record.born, False)
        dbf.write(
            record,
            name = None,
            born = None,
            )
        self.assertTrue(record.name is None)
        self.assertTrue(record.born is False)
    def test_null_type(self):
        "NullType"
        from pprint import pprint
        table = Table(
            filename=':memory:',
            field_specs='name C(20) null; born L null; married D null; appt T null; wisdom M null',
            default_data_types=dict(
                    C=(Char, NoneType, NullType),
                    L=(Logical, NoneType, NullType),
                    D=(Date, NoneType, NullType),
                    T=(DateTime, NoneType, NullType),
                    M=(Char, NoneType, NullType),
                    ),
            dbf_type='vfp',
            )
        table.open()
        table.append()
        record = table[-1]
        self.assertEqual(record.name, None)
        self.assertEqual(record.born, None)
        self.assertEqual(record.married, None)
        self.assertEqual(record.appt, None)
        self.assertEqual(record.wisdom, None)
        appt = datetime.datetime(2012, 12, 15, 9, 37, 11)
        dbf.write(
                record,
                name = 'Ethan               ',
                born = True,
                married = datetime.date(2001, 6, 27),
                appt = appt,
                wisdom = 'timing is everything',
                )
        self.assertEqual(record.name, 'Ethan')
        self.assertEqual(type(record.name), Char)
        self.assertTrue(record.born)
        self.assertTrue(record.born is Truth)
        self.assertEqual(record.married, datetime.date(2001, 6, 27))
        self.assertEqual(type(record.married), Date)
        self.assertEqual(record.appt, datetime.datetime(2012, 12, 15, 9, 37, 11))
        self.assertEqual(type(record.appt), DateTime)
        self.assertEqual(record.wisdom, 'timing is everything')
        self.assertEqual(type(record.wisdom), Char)
        dbf.write(record, name=Null, born=Null, married=Null, appt=Null, wisdom=Null)
        self.assertTrue(record.name is Null)
        self.assertTrue(record.born is Null)
        self.assertTrue(record.born is Null)
        self.assertTrue(record.married is Null)
        self.assertTrue(record.appt is Null)
        self.assertTrue(record.wisdom is Null)
        dbf.write(
                record,
                name = None,
                born = None,
                married = None,
                appt = None,
                wisdom = None,
                )
        self.assertTrue(record.name is None)
        self.assertTrue(record.born is None)
        self.assertTrue(record.married is None)
        self.assertTrue(record.appt is None)
        self.assertTrue(record.wisdom is None)

    def test_nonascii_text_cptrans(self):
        "check non-ascii text to unicode"
        table = Table(':memory:', 'data C(50); memo M', codepage='cp437', dbf_type='vfp')
        table.open()
        decoder = codecs.getdecoder('cp437')
        high_ascii = decoder(''.join(chr(c) for c in range(128, 128+50)))[0]
        table.append(dict(data=high_ascii, memo=high_ascii))
        self.assertEqual(table[0].data, high_ascii)
        self.assertEqual(table[0].memo, high_ascii)
        table.close()

    def test_nonascii_text_no_cptrans(self):
        "check non-ascii text to bytes"
        table = Table(':memory:', 'bindata C(50) binary; binmemo M binary', codepage='cp1252', dbf_type='vfp')
        table.open()
        high_ascii = ''.join(chr(c) for c in range(128, 128+50))
        table.append(dict(bindata=high_ascii, binmemo=high_ascii))
        self.assertEqual(table[0].bindata, ''.join(chr(c) for c in range(128, 128+50)))
        self.assertEqual(table[0].binmemo, ''.join(chr(c) for c in range(128, 128+50)))
        table.close()

    def test_add_null_field(self):
        "adding a null field to an existing table"
        table = Table(
            self.vfp_table.filename,
            'name C(50); age N(3,0)',
            dbf_type='vfp',
            )
        table.open()
        def _50(text):
            return text + ' ' * (50 - len(text))
        data = ( (_50('Ethan'), 29), (_50('Joseph'), 33), (_50('Michael'), 54), )
        for datum in data:
            table.append(datum)
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum, tuple(recordnum))
        table.add_fields('fired D null')
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum, tuple(recordnum)[:2])
        data += ((_50('Daniel'), 44, Null), )
        table.append(('Daniel', 44, Null))
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[:2], tuple(recordnum)[:2])
        self.assertTrue(datum[2] is recordnum[2])
        table.close()
        table = Table(table.filename)
        table.open()
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[0:2], tuple(recordnum)[:2])
        self.assertTrue(datum[2] is recordnum[2])
        table.close()

    def test_remove_null_field(self):
        "removing null fields from an existing table"
        table = Table(
            self.vfp_table.filename,
            'name C(50); age N(3,0); fired D null',
            dbf_type='vfp',
            )
        table.open()
        def _50(text):
            return text + ' ' * (50 - len(text))
        data = ( (_50('Ethan'), 29, Null), (_50('Joseph'), 33, Null), (_50('Michael'), 54, Date(2010, 5, 3)))
        for datum in data:
            table.append(datum)
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[:2], tuple(recordnum)[:2])
            self.assertTrue(datum[2] is recordnum[2] or datum[2] == recordnum[2])
        table.delete_fields('fired')
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[:2], tuple(recordnum))
        data += ((_50('Daniel'), 44), )
        table.append(('Daniel', 44))
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[:2], tuple(recordnum))
        table.close()
        table = Table(table.filename)
        table.open()
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[:2], tuple(recordnum))
        table.close()

    def test_change_null_field(self):
        "making an existing field nullable (or not)"
        table = self.vfp_table
        table.open()
        namelist = []
        paidlist = []
        qtylist = []
        orderlist = []
        desclist = []
        for i in range(10):
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            namelist.append(unicode(name))
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(desc)
            table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc})
        # plus a blank record
        namelist.append('')
        paidlist.append(None)
        qtylist.append(None)
        orderlist.append(None)
        desclist.append('')
        table.append()
        for field in table.field_names:
            self.assertEqual(table.nullable_field(field), False)
        self.assertRaises(FieldMissingError, table.allow_nulls, 'not_here')
        table.allow_nulls('name, qty')
        for field in table.field_names:
            if field in 'name qty'.split():
                self.assertEqual(table.nullable_field(field), True)
            else:
                self.assertEqual(table.nullable_field(field), False)
        dbf.write(table[0], name=Null, qty=Null)
        self.assertTrue(table[0].name is Null)
        self.assertTrue(table[0].qty is Null)
        self.assertRaises(FieldMissingError, table.disallow_nulls, 'not_here')
        table.disallow_nulls('qty')
        for field in table.field_names:
            if field in 'name'.split():
                self.assertEqual(table.nullable_field(field), True)
            else:
                self.assertEqual(table.nullable_field(field), False)
        table.disallow_nulls('name')
        for field in table.field_names:
            self.assertEqual(table.nullable_field(field), False)
        table.close()

    def test_add_field_to_null(self):
        "adding a normal field to a table with null fields"
        table = Table(
            self.vfp_table.filename,
            'name C(50); age N(3,0); fired D null',
            dbf_type='vfp',
            )
        table.open()
        def _50(text):
            return text + ' ' * (50 - len(text))
        data = ( (_50('Ethan'), 29, Null), (_50('Joseph'), 33, Null), (_50('Michael'), 54, Date(2010, 7, 4)), )
        for datum in data:
            table.append(datum)
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[:2], tuple(recordnum)[:2])
            self.assertTrue(datum[2] is recordnum[2] or datum[2] == recordnum[2])
        table.add_fields('tenure N(3,0)')
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[:2], tuple(recordnum)[:2])
            self.assertTrue(datum[2] is recordnum[2] or datum[2] == recordnum[2])
        data += ((_50('Daniel'), 44, Date(2005, 1, 31), 15 ), )
        table.append(('Daniel', 44, Date(2005, 1, 31), 15))
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[:2], tuple(recordnum)[:2])
            self.assertTrue(datum[2] is recordnum[2] or datum[2] == recordnum[2])
        self.assertEqual(datum[3], recordnum[3])
        table.close()
        table = Table(table.filename)
        table.open()
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[:2], tuple(recordnum)[:2])
            self.assertTrue(datum[2] is recordnum[2] or datum[2] == recordnum[2])
        self.assertEqual(datum[3], recordnum[3])
        table.close()

    def test_remove_field_from_null(self):
        "removing a normal field from a table with null fields"
        mfd = self.vfp_table._meta.mfd
        mfd = None
        table = Table(
            self.vfp_table.filename,
            'name C(50); age N(3,0); fired D null',
            dbf_type='vfp',
            )
        table.open()
        def _50(text):
            return text + ' ' * (50 - len(text))
        data = ( (_50('Ethan'), 29, Null), (_50('Joseph'), 33, Null), (_50('Michael'), 54, Date(2010, 7, 4)), )
        for datum in data:
            table.append(datum)
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[:2], tuple(recordnum)[:2])
            self.assertTrue(datum[2] is recordnum[2] or datum[2] == recordnum[2])
        table.delete_fields('age')
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[0], recordnum[0])
            self.assertTrue(datum[-1] is recordnum[1] or datum[-1] == recordnum[1])
        data += ((_50('Daniel'), Date(2001, 11, 13)), )
        table.append(('Daniel', Date(2001, 11, 13)))
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[0], recordnum[0])
            self.assertTrue(datum[-1] is recordnum[1] or datum[-1] == recordnum[1])
        table.close()
        table = Table(table.filename)
        table.open()
        for datum, recordnum in zip(data, table):
            self.assertEqual(datum[0], recordnum[0])
            self.assertTrue(datum[-1] is recordnum[-1] or datum[-1] == recordnum[-1], "name = %s; datum[-1] = %r;  recordnum[-1] = %r" % (datum[0], datum[-1], recordnum[-1]))
        table.close()
    def test_flux_internal(self):
        "commit and rollback of flux record (implementation detail)"
        table = self.dbf_table
        table.open()
        table.append(('dbf master', True, 77, Date(2012, 5, 20), "guru of some things dbf-y"))
        record = table[-1]
        old_data = dbf.scatter(record)
        record._start_flux()
        record.name = 'novice'
        record.paid = False
        record.qty = 69
        record.orderdate = Date(2011, 1, 1)
        record.desc = 'master of all he surveys'
        self.assertEqual(
                dbf.scatter(record),
                dict(
                    name=u'novice                   ',
                    paid=False,
                    qty=69,
                    orderdate=datetime.date(2011, 1, 1),
                    desc='master of all he surveys',
                    ))
        record._rollback_flux()
        self.assertEqual(old_data, dbf.scatter(record))
        record._start_flux()
        record.name = 'novice'
        record.paid = False
        record.qty = 69
        record.orderdate = Date(2011, 1, 1)
        record._commit_flux()
        self.assertEqual(
                dbf.scatter(record),
                dict(
                    name=u'novice                   ',
                    paid=False,
                    qty=69,
                    orderdate=datetime.date(2011, 1, 1),
                    desc='guru of some things dbf-y',
                    ))
        self.assertNotEqual(old_data, dbf.scatter(record))

class Test_Dbf_Functions(unittest.TestCase):
    def setUp(self):
        "create a dbf and vfp table"
        self.dbf_table = table = Table(
            os.path.join(tempdir, 'temptable'),
            'name C(25); paid L; qty N(11,5); orderdate D; desc M', dbf_type='db3'
            )
        table.open()
        namelist = self.dbf_namelist = []
        paidlist = self.dbf_paidlist = []
        qtylist = self.dbf_qtylist = []
        orderlist = self.dbf_orderlist = []
        desclist = self.dbf_desclist = []
        for i in range(len(floats)):
            name = '%-25s' % words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            namelist.append(name)
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(desc)
            table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc})
        table.close()

        self.vfp_table = table = Table(
                os.path.join(tempdir, 'tempvfp'),
                'name C(25); paid L; qty N(11,5); orderdate D; desc M; mass B;'
                ' weight F(18,3); age I; meeting T; misc G; photo P',
                dbf_type='vfp',
                )
        table.open()
        namelist = self.vfp_namelist = []
        paidlist = self.vfp_paidlist = []
        qtylist = self.vfp_qtylist = []
        orderlist = self.vfp_orderlist = []
        desclist = self.vfp_desclist = []
        masslist = self.vfp_masslist = []
        weightlist = self.vfp_weightlist = []
        agelist = self.vfp_agelist = []
        meetlist = self.vfp_meetlist = []
        misclist = self.vfp_misclist = []
        photolist = self.vfp_photolist = []
        for i in range(len(floats)):
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            mass = floats[i] * floats[i] / 2.0
            weight = floats[i] * 3
            age = numbers[i]
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            mass = floats[i] * floats[i] / 2.0
            weight = floats[i] * 3
            age = numbers[i]
            meeting = datetime.datetime((numbers[i] + 2000), (numbers[i] % 12)+1, (numbers[i] % 28)+1, \
                      (numbers[i] % 24), numbers[i] % 60, (numbers[i] * 3) % 60)
            misc = ' '.join(words[i:i+50:3])
            photo = ' '.join(words[i:i+50:7])
            namelist.append('%-25s' % name)
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(desc)
            masslist.append(mass)
            weightlist.append(weight)
            agelist.append(age)
            meetlist.append(meeting)
            misclist.append(misc)
            photolist.append(photo)
            meeting = datetime.datetime((numbers[i] + 2000), (numbers[i] % 12)+1, (numbers[i] % 28)+1,
                      (numbers[i] % 24), numbers[i] % 60, (numbers[i] * 3) % 60)
            misc = ' '.join(words[i:i+50:3])
            photo = ' '.join(words[i:i+50:7])
            table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc,
                    'mass':mass, 'weight':weight, 'age':age, 'meeting':meeting, 'misc':misc, 'photo':photo})
        table.close()
    def tearDown(self):
        self.dbf_table.close()
        self.vfp_table.close()

    def test_add_fields_to_dbf_table(self):
        "dbf table:  adding and deleting fields"
        table = self.dbf_table
        table.open()
        dbf._debug = True
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist
        table.delete_fields('name')
        table.close()
        table = Table(table.filename, dbf_type='db3')
        table.open()
        for field in table.field_names:
            self.assertEqual(1, table.field_names.count(field))
        i = 0
        for record in table:
            self.assertEqual(dbf.recno(record), i)

            self.assertEqual(table[i].paid, paidlist[i])
            self.assertEqual(record.paid, paidlist[i])
            self.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            self.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            self.assertEqual(table[i].orderdate, orderlist[i])
            self.assertEqual(record.orderdate, orderlist[i])
            self.assertEqual(table[i].desc, desclist[i])
            self.assertEqual(record.desc, desclist[i])
            i += 1
        first, middle, last = table[0], table[len(table)//2], table[-1]
        table.delete_fields('paid, orderdate')
        for field in table.field_names:
            self.assertEqual(1, table.field_names.count(field))
        i = 0
        for record in table:
            self.assertEqual(dbf.recno(record), i)
            self.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            self.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            self.assertEqual(table[i].desc, desclist[i])
            self.assertEqual(record.desc, desclist[i])
            i += 1
        self.assertEqual(i, len(table))
        self.assertTrue('paid' not in dbf.field_names(first))
        self.assertTrue('orderdate' not in dbf.field_names(first))
        table.add_fields('name C(25); paid L; orderdate D')
        for field in table.field_names:
            self.assertEqual(1, table.field_names.count(field))
        self.assertEqual(i, len(table))
        i = 0
        for i, record in enumerate(table):
            self.assertEqual(record.name, ' ' * 25)
            self.assertEqual(record.paid, None)
            self.assertEqual(record.orderdate, None)
            self.assertEqual(record.desc, desclist[i])
            i += 1
        self.assertEqual(i, len(table))
        i = 0
        for record in table:
            data = dict()
            data['name'] = namelist[dbf.recno(record)]
            data['paid'] = paidlist[dbf.recno(record)]
            data['orderdate'] = orderlist[dbf.recno(record)]
            dbf.gather(record, data)
            i += 1
        self.assertEqual(i, len(table))
        i = 0
        for record in table:
            self.assertEqual(dbf.recno(record), i)
            self.assertEqual(table[i].name, namelist[i])
            self.assertEqual(record.name, namelist[i])
            self.assertEqual(table[i].paid, paidlist[i])
            self.assertEqual(record.paid, paidlist[i])
            self.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            self.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            self.assertEqual(table[i].orderdate, orderlist[i])
            self.assertEqual(record.orderdate, orderlist[i])
            self.assertEqual(table[i].desc, desclist[i])
            self.assertEqual(record.desc, desclist[i])
            i += 1
        table.close()
    def test_add_fields_to_vfp_table(self):
        "vfp table:  adding and deleting fields"
        table = self.vfp_table
        table.open()
        namelist = self.vfp_namelist
        paidlist = self.vfp_paidlist
        qtylist = self.vfp_qtylist
        orderlist = self.vfp_orderlist
        desclist = self.vfp_desclist
        masslist = self.vfp_masslist
        weightlist = self.vfp_weightlist
        agelist = self.vfp_agelist
        meetlist = self.vfp_meetlist
        misclist = self.vfp_misclist
        photolist = self.vfp_photolist
        self.assertEqual(len(table), len(floats))
        i = 0
        for record in table:
            self.assertEqual(dbf.recno(record), i)
            self.assertEqual(table[i].name, namelist[i])
            self.assertEqual(record.name, namelist[i])
            self.assertEqual(table[i].paid, paidlist[i])
            self.assertEqual(record.paid, paidlist[i])
            self.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            self.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            self.assertEqual(table[i].orderdate, orderlist[i])
            self.assertEqual(record.orderdate, orderlist[i])
            self.assertEqual(table[i].desc, desclist[i])
            self.assertEqual(record.desc, desclist[i])
            self.assertEqual(record.mass, masslist[i])
            self.assertEqual(table[i].mass, masslist[i])
            self.assertEqual(record.weight, weightlist[i])
            self.assertEqual(table[i].weight, weightlist[i])
            self.assertEqual(record.age, agelist[i])
            self.assertEqual(table[i].age, agelist[i])
            self.assertEqual(record.meeting, meetlist[i])
            self.assertEqual(table[i].meeting, meetlist[i])
            self.assertEqual(record.misc, misclist[i])
            self.assertEqual(table[i].misc, misclist[i])
            self.assertEqual(record.photo, photolist[i])
            self.assertEqual(table[i].photo, photolist[i])
            i += 1
        table.delete_fields('desc')
        i = 0
        for record in table:
            self.assertEqual(dbf.recno(record), i)
            self.assertEqual(table[i].name, namelist[i])
            self.assertEqual(record.name, namelist[i])
            self.assertEqual(table[i].paid, paidlist[i])
            self.assertEqual(record.paid, paidlist[i])
            self.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            self.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            self.assertEqual(table[i].orderdate, orderlist[i])
            self.assertEqual(record.orderdate, orderlist[i])
            self.assertEqual(record.weight, weightlist[i])
            self.assertEqual(table[i].weight, weightlist[i])
            self.assertEqual(record.age, agelist[i])
            self.assertEqual(table[i].age, agelist[i])
            self.assertEqual(record.meeting, meetlist[i])
            self.assertEqual(table[i].meeting, meetlist[i])
            self.assertEqual(record.misc, misclist[i])
            self.assertEqual(table[i].misc, misclist[i])
            self.assertEqual(record.photo, photolist[i])
            self.assertEqual(table[i].photo, photolist[i])
            self.assertEqual(record.mass, masslist[i])
            self.assertEqual(table[i].mass, masslist[i])
            i += 1
        table.delete_fields('paid, mass')
        i = 0
        for record in table:
            self.assertEqual(dbf.recno(record), i)
            self.assertEqual(table[i].name, namelist[i])
            self.assertEqual(record.name, namelist[i])
            self.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            self.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            self.assertEqual(table[i].orderdate, orderlist[i])
            self.assertEqual(record.orderdate, orderlist[i])
            self.assertEqual(record.weight, weightlist[i])
            self.assertEqual(table[i].weight, weightlist[i])
            self.assertEqual(record.age, agelist[i])
            self.assertEqual(table[i].age, agelist[i])
            self.assertEqual(record.meeting, meetlist[i])
            self.assertEqual(table[i].meeting, meetlist[i])
            self.assertEqual(record.misc, misclist[i])
            self.assertEqual(table[i].misc, misclist[i])
            self.assertEqual(record.photo, photolist[i])
            self.assertEqual(table[i].photo, photolist[i])
            i += 1
        table.add_fields('desc M; paid L; mass B')
        i = 0
        for record in table:
            self.assertEqual(record.desc, u'')
            self.assertEqual(record.paid is None, True)
            self.assertEqual(record.mass, 0.0)
            i += 1
        self.assertEqual(i, len(table))
        i = 0
        for record in Process(table):
            record.desc = desclist[dbf.recno(record)]
            record.paid = paidlist[dbf.recno(record)]
            record.mass = masslist[dbf.recno(record)]
            i += 1
        self.assertEqual(i, len(table))
        i = 0
        for record in table:
            self.assertEqual(dbf.recno(record), i)
            self.assertEqual(table[i].name, namelist[i])
            self.assertEqual(record.name, namelist[i])
            self.assertEqual(table[i].paid, paidlist[i])
            self.assertEqual(record.paid, paidlist[i])
            self.assertEqual(abs(table[i].qty - qtylist[i]) < .00001, True)
            self.assertEqual(abs(record.qty - qtylist[i]) < .00001, True)
            self.assertEqual(table[i].orderdate, orderlist[i])
            self.assertEqual(record.orderdate, orderlist[i])
            self.assertEqual(table[i].desc, desclist[i])
            self.assertEqual(record.desc, desclist[i])
            self.assertEqual(record.mass, masslist[i])
            self.assertEqual(table[i].mass, masslist[i])
            self.assertEqual(record.weight, weightlist[i])
            self.assertEqual(table[i].weight, weightlist[i])
            self.assertEqual(record.age, agelist[i])
            self.assertEqual(table[i].age, agelist[i])
            self.assertEqual(record.meeting, meetlist[i])
            self.assertEqual(table[i].meeting, meetlist[i])
            self.assertEqual(record.misc, misclist[i])
            self.assertEqual(table[i].misc, misclist[i])
            self.assertEqual(record.photo, photolist[i])
            self.assertEqual(table[i].photo, photolist[i])
            i += 1
        table.close()
    def test_len_contains_iter(self):
        "basic function tests - len, contains & iterators"
        table = self.dbf_table
        table.open()
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist
        for field in table.field_names:
            self.assertEqual(1, table.field_names.count(field))
        length = sum([1 for rec in table])
        self.assertEqual(length, len(table))
        i = 0
        for record in table:
            self.assertEqual(record, table[i])
            self.assertTrue(record in table)
            self.assertTrue(tuple(record) in table)
            self.assertTrue(scatter(record) in table)
            self.assertTrue(create_template(record) in table)
            i += 1
        self.assertEqual(i, len(table))
        table.close()

    def test_un_delete(self):
        "delete, undelete"
        table = Table(':memory:', 'name C(10)', dbf_type='db3')
        table.open()
        table.append()
        self.assertEqual(table.next_record, table[0])
        table = Table(':memory:', 'name C(10)', dbf_type='db3')
        table.open()
        table.append(multiple=10)
        self.assertEqual(table.next_record, table[0])
        table = self.dbf_table              # Table(os.path.join(tempdir, 'temptable'), dbf_type='db3')
        table.open()
        total = len(table)
        table.bottom()
        self.assertEqual(dbf.recno(table.current_record), total)
        table.top()
        self.assertEqual(dbf.recno(table.current_record), -1)
        table.goto(27)
        self.assertEqual(dbf.recno(table.current_record), 27)
        table.goto(total-1)
        self.assertEqual(dbf.recno(table.current_record), total-1)
        table.goto(0)
        self.assertEqual(dbf.recno(table.current_record), 0)
        self.assertRaises(IndexError, table.goto, total)
        self.assertRaises(IndexError, table.goto, -len(table)-1)
        table.top()
        self.assertRaises(dbf.Bof, table.skip, -1)
        table.bottom()
        self.assertRaises(Eof, table.skip)
        for record in table:
            dbf.delete(record)
        active_records = table.create_index(active)
        active_records.top()
        self.assertRaises(Eof, active_records.skip)
        dbf._debug = True
        active_records.bottom()
        self.assertRaises(Bof, active_records.skip, -1)
        for record in table:
            dbf.undelete(record)

        # delete every third record
        i = 0
        for record in table:
            self.assertEqual(dbf.recno(record), i)
            if i % 3 == 0:
                dbf.delete(record)
            i += 1
        i = 0
        # and verify
        for record in table:
            self.assertEqual(dbf.is_deleted(record), i%3==0)
            self.assertEqual(dbf.is_deleted(table[i]), i%3==0)
            i += 1

        # check that deletes were saved to disk..
        table.close()
        table = Table(os.path.join(tempdir, 'temptable'), dbf_type='db3')
        table.open()
        active_records = table.create_index(active)
        i = 0
        for record in table:
            self.assertEqual(dbf.is_deleted(record), i%3==0)
            self.assertEqual(dbf.is_deleted(table[i]), i%3==0)
            i += 1

        # verify record numbers
        i = 0
        for record in table:
            self.assertEqual(dbf.recno(record), i)
            i += 1

        # verify that deleted records are skipped
        i = 0
        for record in active_records:
            self.assertNotEqual(dbf.recno(record)%3, 0)
        active_records.goto(1)
        active_records.skip()
        self.assertEqual(dbf.recno(active_records.current_record), 4)
        active_records.skip(-1)
        self.assertEqual(dbf.recno(active_records.current_record), 2)

        # verify that deleted records are skipped in slices
        list_of_records = active_records[3:6]
        self.assertEqual(len(list_of_records), 3)
        self.assertEqual(dbf.recno(list_of_records[0]), 5)
        self.assertEqual(dbf.recno(list_of_records[1]), 7)
        self.assertEqual(dbf.recno(list_of_records[2]), 8)

        # verify behavior when all records are deleted
        for record in table:
            dbf.delete(record)
        active_records.bottom()
        self.assertRaises(Eof, active_records.skip)
        self.assertEqual(active_records.eof, True)
        active_records.top()
        self.assertRaises(Bof, active_records.skip, -1)
        self.assertEqual(active_records.bof, True)

        # verify deleted records are seen with active record index
        deleted_records = table.create_index(inactive)
        i = 0
        for record in deleted_records:
            self.assertEqual(dbf.recno(record), i)
            i += 1

        # verify undelete using table[index]
        for record in table:
            dbf.delete(record)
            self.assertTrue(dbf.is_deleted(record))
        for i, record in enumerate(table):
            dbf.undelete(table[i])
            self.assertEqual(dbf.is_deleted(record), False)
            self.assertEqual(dbf.is_deleted(table[i]), False)
            self.assertFalse(record in deleted_records)

        # verify all records have been undeleted (recalled)
        self.assertEqual(len(active_records), len(table))
        self.assertEqual(len(deleted_records), 0)
        table.close()

    def test_finding_ordering_searching(self):
        "finding, ordering, searching"
        table = self.dbf_table
        table.open()
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist

        # find (brute force)
        unordered = []
        for record in table:
            unordered.append(record.name)
        for word in unordered:                                  # returns records
            records = table.query("select * where name == %r" % word)
            self.assertEqual(len(records), unordered.count(word))
            records = [rec for rec in table if rec.name == word]
            self.assertEqual(len(records), unordered.count(word))

        # ordering by one field
        ordered = unordered[:]
        ordered.sort()
        name_index = table.create_index(lambda rec: rec.name)
        self.assertEqual(list(name_index[::-1]), list(reversed(name_index)))
        i = 0
        for record in name_index:
            self.assertEqual(record.name, ordered[i])
            i += 1

        # search (binary)
        for word in unordered:
            records = name_index.search(match=word)
            self.assertEqual(len(records), unordered.count(word), "num records: %d\nnum words: %d\nfailure with %r" % (len(records), unordered.count(word), word))
            records = table.query("select * where name == %r" % word)
            self.assertEqual(len(records), unordered.count(word))
            records = dbf.pql(table, "select * where name == %r" % word)
            self.assertEqual(len(records), unordered.count(word))

        # ordering by two fields
        ordered = unordered[:]
        ordered.sort()
        nd_index = table.create_index(lambda rec: (rec.name, rec.desc))
        self.assertEqual(list(nd_index[::-1]), list(reversed(nd_index)))
        i = 0
        for record in nd_index:
            self.assertEqual(record.name, ordered[i])
            i += 1

        # search (binary)
        for word in unordered:
            records = nd_index.search(match=(word, ), partial=True)
            ucount = sum([1 for wrd in unordered if wrd.startswith(word)])
            self.assertEqual(len(records), ucount)

        for record in table[::2]:
            dbf.write(record, qty=-record.qty)
        unordered = []
        for record in table:
            unordered.append(record.qty)
        ordered = unordered[:]
        ordered.sort()
        qty_index = table.create_index(lambda rec: rec.qty)
        self.assertEqual(list(qty_index[::-1]), list(reversed(qty_index)))
        i = 0
        for record in qty_index:
            self.assertEqual(record.qty, ordered[i])
            i += 1
        for number in unordered:
            records = qty_index.search(match=(number, ))
            self.assertEqual(len(records), unordered.count(number))

        table.close()
    def test_scatter_gather_new(self):
        "scattering and gathering fields, and new()"
        table = self.dbf_table
        table.open()
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist
        table2 = table.new(os.path.join(tempdir, 'temptable2'))
        table2.open()
        for record in table:
            table2.append()
            newrecord = table2[-1]
            testdict = dbf.scatter(record)
            for key in field_names(testdict):
                self.assertEqual(testdict[key], record[key])
            dbf.gather(newrecord, dbf.scatter(record))
            for field in dbf.field_names(record):
                self.assertEqual(newrecord[field], record[field])
        table2.close()
        table2 = None
        table2 = Table(os.path.join(tempdir, 'temptable2'), dbf_type='db3')
        table2.open()
        for i in range(len(table)):
            temp1 = dbf.scatter(table[i])
            temp2 = dbf.scatter(table2[i])
            for key in field_names(temp1):
                self.assertEqual(temp1[key], temp2[key])
            for key in field_names(temp2):
                self.assertEqual(temp1[key], temp2[key])
        table2.close()
        table3 = table.new(':memory:')
        table3.open()
        for record in table:
            table3.append(record)
        table4 = self.vfp_table
        table4.open()
        table5 = table4.new(':memory:')
        table5.open()
        for record in table4:
            table5.append(record)
        table.close()
        table3.close()
        table4.close()
        table5.close()
    def test_rename_contains_has_key(self):
        "renaming fields, __contains__, has_key"
        table = self.dbf_table
        table.open()
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist
        for field in table.field_names:
            oldfield = field
            table.rename_field(oldfield, 'newfield')
            self.assertEqual(oldfield in table.field_names, False)
            self.assertEqual('newfield' in table.field_names, True)
            table.close()
            table = Table(os.path.join(tempdir, 'temptable'), dbf_type='db3')
            table.open()
            self.assertEqual(oldfield in table.field_names, False)
            self.assertEqual('newfield' in table.field_names, True)
            table.rename_field('newfield', oldfield)
            self.assertEqual(oldfield in table.field_names, True)
            self.assertEqual('newfield' in table.field_names, False)
        table.close()

    def test_dbf_record_kamikaze(self):
        "kamikaze"
        table = self.dbf_table
        table.open()
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist
        table2 = table.new(os.path.join(tempdir, 'temptable2'))
        table2.open()
        for record in table:
            table2.append(record)
            newrecord = table2[-1]
            for key in table.field_names:
                if key not in table.memo_types:
                    self.assertEqual(newrecord[key], record[key])
            for field in dbf.field_names(newrecord):
                if key not in table2.memo_types:
                    self.assertEqual(newrecord[field], record[field])
        table2.close()
        table2 = Table(os.path.join(tempdir, 'temptable2'), dbf_type='db3')
        table2.open()
        for i in range(len(table)):
            dict1 = dbf.scatter(table[i], as_type=dict)
            dict2 = dbf.scatter(table2[i], as_type=dict)
            for key in dict1.keys():
                if key not in table.memo_types:
                    self.assertEqual(dict1[key], dict2[key])
            for key in dict2.keys():
                if key not in table2.memo_types:
                    self.assertEqual(dict1[key], dict2[key])
        for i in range(len(table)):
            template1 = dbf.scatter(table[i])
            template2 = dbf.scatter(table2[i])
            for key in dbf.field_names(template1):
                if key not in table.memo_types:
                    self.assertEqual(template1[key], template2[key])
            for key in dbf.field_names(template2):
                if key not in table2.memo_types:
                    self.assertEqual(template1[key], template2[key])
        table.close()
        table2.close()

    def test_multiple_append(self):
        "multiple append"
        table = self.dbf_table
        table.open()
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist
        table2 = table.new(os.path.join(tempdir, 'temptable2'))
        table2.open()
        record = table.next_record
        table2.append(dbf.scatter(record), multiple=100)
        for samerecord in table2:
            for field in dbf.field_names(record):
                self.assertEqual(record[field], samerecord[field])
        table2.close()
        table2 = Table(os.path.join(tempdir, 'temptable2'), dbf_type='db3')
        table2.open()
        for samerecord in table2:
            for field in dbf.field_names(record):
                self.assertEqual(record[field], samerecord[field])
        table2.close()
        table3 = table.new(os.path.join(tempdir, 'temptable3'))
        table3.open()
        record = table.next_record
        table3.append(record, multiple=100)
        for samerecord in table3:
            for field in dbf.field_names(record):
                self.assertEqual(record[field], samerecord[field])
        table3.close()
        table3 = Table(os.path.join(tempdir, 'temptable3'), dbf_type='db3')
        table3.open()
        for samerecord in table3:
            for field in dbf.field_names(record):
                self.assertEqual(record[field], samerecord[field])
        table3.close()
        table.close()
    def test_slices(self):
        "slices"
        table = self.dbf_table
        table.open()
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist
        slice1 = [table[0], table[1], table[2]]
        self.assertEqual(slice1, list(table[:3]))
        slice2 = [table[-3], table[-2], table[-1]]
        self.assertEqual(slice2, list(table[-3:]))
        slice3 = [record for record in table]
        self.assertEqual(slice3, list(table[:]))
        slice4 = [table[9]]
        self.assertEqual(slice4, list(table[9:10]))
        slice5 = [table[15], table[16], table[17], table[18]]
        self.assertEqual(slice5, list(table[15:19]))
        slice6 = [table[0], table[2], table[4], table[6], table[8]]
        self.assertEqual(slice6, list(table[:9:2]))
        slice7 = [table[-1], table[-2], table[-3]]
        self.assertEqual(slice7, list(table[-1:-4:-1]))
        table.close()
    def test_record_reset(self):
        "reset record"
        table = self.dbf_table
        table.open()
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist
        for record in table:
            dbf.reset(record)
            dbf.write(record)
        self.assertEqual(table[0].name, table[1].name)
        dbf.write(table[0], name='Python rocks!')
        self.assertNotEqual(table[0].name, table[1].name)
        table.close()
    def test_adding_memos(self):
        "adding memos to existing records"
        table = Table(':memory:', 'name C(50); age N(3,0)', dbf_type='db3')
        table.open()
        table.append(('user', 0))
        table.add_fields('motto M')
        dbf.write(table[0], motto='Are we there yet??')
        self.assertEqual(table[0].motto, 'Are we there yet??')
        table.close()
        table = Table(os.path.join(tempdir, 'temptable4'), 'name C(50); age N(3,0)', dbf_type='db3')
        table.open()
        table.append(('user', 0))
        table.close()
        table.open()
        table.close()
        table = Table(os.path.join(tempdir, 'temptable4'), dbf_type='db3')
        table.open()
        table.add_fields('motto M')
        dbf.write(table[0], motto='Are we there yet??')
        self.assertEqual(table[0].motto, 'Are we there yet??')
        table.close()
        table = Table(os.path.join(tempdir, 'temptable4'), dbf_type='db3')
        table.open()
        self.assertEqual(table[0].motto, 'Are we there yet??')
        table.close()
        table = Table(os.path.join(tempdir, 'temptable4'), 'name C(50); age N(3,0)', dbf_type='vfp')
        table.open()
        table.append(('user', 0))
        table.close()
        table.open()
        table.close()
        table = Table(os.path.join(tempdir, 'temptable4'), dbf_type='vfp')
        table.open()
        table.add_fields('motto M')
        dbf.write(table[0], motto='Are we there yet??')
        self.assertEqual(table[0].motto, 'Are we there yet??')
        table.close()
        table = Table(os.path.join(tempdir, 'temptable4'), dbf_type='vfp')
        table.open()
        self.assertEqual(table[0].motto, 'Are we there yet??')
        table.close()
    def test_from_csv(self):
        "from_csv"
        table = self.dbf_table
        table.open()
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist
        dbf.export(table, table.filename, header=False)
        csvtable = dbf.from_csv(os.path.join(tempdir, 'temptable.csv'))
        csvtable.open()
        for i in index(table):
            for j in index(table.field_names):
                self.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable.close()
        csvtable = dbf.from_csv(os.path.join(tempdir, 'temptable.csv'), to_disk=True, filename=os.path.join(tempdir, 'temptable5'))
        csvtable.open()
        for i in index(table):
            for j in index(table.field_names):
                self.assertEqual(str(table[i][j]).strip(), csvtable[i][j].strip())
        csvtable.close()
        csvtable = dbf.from_csv(os.path.join(tempdir, 'temptable.csv'), field_names=['field1','field2'])
        csvtable.open()
        for i in index(table):
            for j in index(table.field_names):
                self.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable.close()
        csvtable = dbf.from_csv(os.path.join(tempdir, 'temptable.csv'), field_names=['field1','field2'], to_disk=True, filename=os.path.join(tempdir, 'temptable5'))
        csvtable.open()
        for i in index(table):
            for j in index(table.field_names):
                self.assertEqual(str(table[i][j]).strip(), csvtable[i][j].strip())
        csvtable.close()
        csvtable = dbf.from_csv(os.path.join(tempdir, 'temptable.csv'), extra_fields=['count N(5,0)','id C(10)'])
        csvtable.open()
        for i in index(table):
            for j in index(table.field_names):
                self.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable.close()
        csvtable = dbf.from_csv(os.path.join(tempdir, 'temptable.csv'), extra_fields=['count N(5,0)','id C(10)'], to_disk=True, filename=os.path.join(tempdir, 'temptable5'))
        csvtable.open()
        for i in index(table):
            for j in index(table.field_names):
                self.assertEqual(str(table[i][j]).strip(), csvtable[i][j].strip())
        csvtable.close()
        csvtable = dbf.from_csv(os.path.join(tempdir, 'temptable.csv'), field_names=['name','qty','paid','desc'], extra_fields='test1 C(15);test2 L'.split(';'))
        csvtable.open()
        for i in index(table):
            for j in index(table.field_names):
                self.assertEqual(str(table[i][j]), csvtable[i][j])
        csvtable.close()
        csvtable = dbf.from_csv(os.path.join(tempdir, 'temptable.csv'), field_names=['name','qty','paid','desc'], extra_fields='test1 C(15);test2 L'.split(';'), to_disk=True, filename=os.path.join(tempdir, 'temptable5'))
        csvtable.open()
        for i in index(table):
            for j in index(table.field_names):
                self.assertEqual(str(table[i][j]).strip(), csvtable[i][j].strip())

    def test_resize(self):
        "resize"
        table = self.dbf_table
        table.open()
        test_record = dbf.scatter(table[5])
        namelist = self.dbf_namelist 
        paidlist = self.dbf_paidlist
        qtylist = self.dbf_qtylist
        orderlist = self.dbf_orderlist
        desclist = self.dbf_desclist
        test_record = dbf.scatter(table[5])
        table.resize_field('name', 40)
        new_record = dbf.scatter(table[5])
        self.assertEqual(test_record['orderdate'], new_record['orderdate'])
        table.close()
    def test_memos_after_close(self):
        "memos available after close/open"
        table = dbf.Table('tempy', 'name C(20); desc M', dbf_type='db3', default_data_types=dict(C=Char))
        table.open()
        table.append(('Author','dashing, debonair, delightful'))
        table.close()
        table.open()
        self.assertEqual(tuple(table[0]), ('Author','dashing, debonair, delightful'))
        table.close()
        table2 = dbf.Table('tempy', 'name C(20); desc M', dbf_type='db3')
        table2.open()
        table2.append(('Benedict', 'brilliant, bombastic, bothered'))
        table2.close()
        table.open()
        self.assertEqual(table[0].name, 'Benedict')
        self.assertEqual(table[0].desc, 'brilliant, bombastic, bothered')
        table.close()
    def test_field_type(self):
        "table.type(field) == ('C', Char)"
        table = dbf.Table('tempy', 'name C(20); desc M', dbf_type='db3', default_data_types=dict(C=Char))
        table.open()
        field_info = table.field_info('name')
        self.assertEqual(field_info, ('C', 20, 0, Char))
        self.assertEqual(field_info.dbf_type, 'C')
        self.assertEqual(field_info.length, 20)
        self.assertEqual(field_info.decimal, 0)
        self.assertEqual(field_info.py_type, Char)
        table.close()

    def test_memo_after_backup(self):
        "memo fields accessible after .backup()"
        table = self.dbf_table
        table.open()
        table.create_backup()
        backup = dbf.Table(table.backup)
        backup.open()
        desclist = self.dbf_desclist
        for i in range(len(desclist)):
            self.assertEqual(desclist[i], backup[i].desc)
        backup.close()
        table.close()
    def test_write_loop(self):
        "Process loop commits changes"
        table = self.dbf_table
        table.open()
        for record in Process(table):
            record.name = '!BRAND NEW NAME!'
        for record in table:
            self.assertEqual(record.name, '!BRAND NEW NAME!         ')
        table.close()
    def test_export(self):
        for table in self.dbf_table, self.vfp_table:
            table.open()
            dbf.export(table, filename='test_export.csv')

class Test_Dbf_Navigation(unittest.TestCase):
    def setUp(self):
        "create a dbf and vfp table"
        self.dbf_table = table = Table(
            os.path.join(tempdir, 'temptable'),
            'name C(25); paid L; qty N(11,5); orderdate D; desc M', dbf_type='db3'
            )
        table.open()
        namelist = self.dbf_namelist = []
        paidlist = self.dbf_paidlist = []
        qtylist = self.dbf_qtylist = []
        orderlist = self.dbf_orderlist = []
        desclist = self.dbf_desclist = []
        for i in range(len(floats)):
            name = '%-25s' % words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            namelist.append(name)
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(desc)
            table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc})
        table.close()

        self.vfp_table = table = Table(
                os.path.join(tempdir, 'tempvfp'),
                'name C(25); paid L; qty N(11,5); orderdate D; desc M; mass B;'
                ' weight F(18,3); age I; meeting T; misc G; photo P',
                dbf_type='vfp',
                )
        table.open()
        namelist = self.vfp_namelist = []
        paidlist = self.vfp_paidlist = []
        qtylist = self.vfp_qtylist = []
        orderlist = self.vfp_orderlist = []
        desclist = self.vfp_desclist = []
        masslist = self.vfp_masslist = []
        weightlist = self.vfp_weightlist = []
        agelist = self.vfp_agelist = []
        meetlist = self.vfp_meetlist = []
        misclist = self.vfp_misclist = []
        photolist = self.vfp_photolist = []
        for i in range(len(floats)):
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            mass = floats[i] * floats[i] / 2.0
            weight = floats[i] * 3
            age = numbers[i]
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            mass = floats[i] * floats[i] / 2.0
            weight = floats[i] * 3
            age = numbers[i]
            meeting = datetime.datetime((numbers[i] + 2000), (numbers[i] % 12)+1, (numbers[i] % 28)+1, \
                      (numbers[i] % 24), numbers[i] % 60, (numbers[i] * 3) % 60)
            misc = ' '.join(words[i:i+50:3])
            photo = ' '.join(words[i:i+50:7])
            namelist.append('%-25s' % name)
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(desc)
            masslist.append(mass)
            weightlist.append(weight)
            agelist.append(age)
            meetlist.append(meeting)
            misclist.append(misc)
            photolist.append(photo)
            meeting = datetime.datetime((numbers[i] + 2000), (numbers[i] % 12)+1, (numbers[i] % 28)+1,
                      (numbers[i] % 24), numbers[i] % 60, (numbers[i] * 3) % 60)
            misc = ' '.join(words[i:i+50:3])
            photo = ' '.join(words[i:i+50:7])
            table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc,
                    'mass':mass, 'weight':weight, 'age':age, 'meeting':meeting, 'misc':misc, 'photo':photo})
        table.close()
    def tearDown(self):
        self.dbf_table.close()
        self.vfp_table.close()

    def test_top(self):
        "top, current in Tables, Lists, and Indexes"
        table = self.dbf_table
        table.open()
        list = List(table)
        index = Index(table, key=lambda rec: dbf.recno(rec))
        total = len(table)
        mid = total // 2
        table.goto(mid)
        list.goto(mid)
        index.goto(mid)
        self.assertTrue(table.current != -1)
        self.assertTrue(list.current != -1)
        self.assertTrue(index.current != -1)
        table.top()
        list.top()
        index.top()
        self.assertEquals(table.current, -1)
        self.assertEquals(list.current, -1)
        self.assertEquals(index.current, -1)

    def test_bottom(self):
        "bottom, current in Tables, Lists, and Indexes"
        table = self.dbf_table
        table.open()
        list = List(table)
        index = Index(table, key=lambda rec: dbf.recno(rec))
        total = len(table)
        mid = total // 2
        table.goto(mid)
        list.goto(mid)
        index.goto(mid)
        self.assertTrue(table.current != -1)
        self.assertTrue(list.current != -1)
        self.assertTrue(index.current != -1)
        table.bottom()
        list.bottom()
        index.bottom()
        self.assertEquals(table.current, total)
        self.assertEquals(list.current, total)
        self.assertEquals(index.current, total)

    def test_goto(self):
        "goto, current in Tables, Lists, and Indexes"
        table = self.dbf_table
        table.open()
        list = List(table)
        index = Index(table, key=lambda rec: dbf.recno(rec))
        total = len(table)
        mid = total // 2
        table.goto(mid)
        list.goto(mid)
        index.goto(mid)
        self.assertEquals(table.current, mid)
        self.assertEquals(list.current, mid)
        self.assertEquals(index.current, mid)
        table.goto('top')
        list.goto('top')
        index.goto('top')
        self.assertEquals(table.current, -1)
        self.assertEquals(list.current, -1)
        self.assertEquals(index.current, -1)
        table.goto('bottom')
        list.goto('bottom')
        index.goto('bottom')
        self.assertEquals(table.current, total)
        self.assertEquals(list.current, total)
        self.assertEquals(index.current, total)
        dbf.delete(table[10])
        self.assertTrue(dbf.is_deleted(list[10]))
        self.assertTrue(dbf.is_deleted(index[10]))
        table.goto(10)
        list.goto(10)
        index.goto(10)
        self.assertEquals(table.current, 10)
        self.assertEquals(list.current, 10)
        self.assertEquals(index.current, 10)
        table.close()

    def test_skip(self):
        "skip, current in Tables, Lists, and Indexes"
        table = self.dbf_table
        table.open()
        list = List(table)
        index = Index(table, key=lambda rec: dbf.recno(rec))
        total = len(table)
        mid = total // 2
        self.assertEquals(table.current, -1)
        self.assertEquals(list.current, -1)
        self.assertEquals(index.current, -1)
        table.skip(1)
        list.skip(1)
        index.skip(1)
        self.assertEquals(table.current, 0)
        self.assertEquals(list.current, 0)
        self.assertEquals(index.current, 0)
        table.skip(10)
        list.skip(10)
        index.skip(10)
        self.assertEquals(table.current, 10)
        self.assertEquals(list.current, 10)
        self.assertEquals(index.current, 10)
        table.close()

    def test_first_record(self):
        "first_record in Tables, Lists, and Indexes"
        table = self.dbf_table
        table.open()
        list = List(table)
        index = Index(table, key=lambda rec: dbf.recno(rec))
        total = len(table)
        mid = total // 2
        self.assertTrue(table[0] is list[0])
        self.assertTrue(table[0] is index[0])
        self.assertTrue(table.first_record is table[0])
        self.assertTrue(list.first_record is table[0])
        self.assertTrue(index.first_record is table[0])
        table.close()

    def test_prev_record(self):
        "prev_record in Tables, Lists, and Indexes"
        table = self.dbf_table
        table.open()
        list = List(table)
        index = Index(table, key=lambda rec: dbf.recno(rec))
        total = len(table)
        mid = total // 2
        self.assertTrue(table[0] is list[0])
        self.assertTrue(table[0] is index[0])
        table.top()
        list.top()
        index.top()
        self.assertTrue(isinstance(table.prev_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(list.prev_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(index.prev_record, dbf.RecordVaporWare))
        table.skip()
        list.skip()
        index.skip()
        self.assertTrue(isinstance(table.prev_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(list.prev_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(index.prev_record, dbf.RecordVaporWare))
        table.skip()
        list.skip()
        index.skip()
        self.assertTrue(table.prev_record is table[0])
        self.assertTrue(list.prev_record is table[0])
        self.assertTrue(index.prev_record is table[0])
        table.close()

    def test_current_record(self):
        "current_record in Tables, Lists, and Indexes"
        table = self.dbf_table
        table.open()
        list = List(table)
        index = Index(table, key=lambda rec: dbf.recno(rec))
        total = len(table)
        mid = total // 2
        table.top()
        list.top()
        index.top()
        self.assertTrue(isinstance(table.current_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(list.current_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(index.current_record, dbf.RecordVaporWare))
        table.bottom()
        list.bottom()
        index.bottom()
        self.assertTrue(isinstance(table.current_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(list.current_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(index.current_record, dbf.RecordVaporWare))
        table.goto(mid)
        list.goto(mid)
        index.goto(mid)
        self.assertTrue(table.current_record is table[mid])
        self.assertTrue(list.current_record is table[mid])
        self.assertTrue(index.current_record is table[mid])
        table.close()

    def test_next_record(self):
        "prev_record in Tables, Lists, and Indexes"
        table = self.dbf_table
        table.open()
        list = List(table)
        index = Index(table, key=lambda rec: dbf.recno(rec))
        total = len(table)
        mid = total // 2
        self.assertTrue(table[0] is list[0])
        self.assertTrue(table[0] is index[0])
        table.bottom()
        list.bottom()
        index.bottom()
        self.assertTrue(isinstance(table.next_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(list.next_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(index.next_record, dbf.RecordVaporWare))
        table.skip(-1)
        list.skip(-1)
        index.skip(-1)
        self.assertTrue(isinstance(table.next_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(list.next_record, dbf.RecordVaporWare))
        self.assertTrue(isinstance(index.next_record, dbf.RecordVaporWare))
        table.skip(-1)
        list.skip(-1)
        index.skip(-1)
        self.assertTrue(table.next_record is table[-1])
        self.assertTrue(list.next_record is table[-1])
        self.assertTrue(index.next_record is table[-1])
        table.close()

    def test_last_record(self):
        "last_record in Tables, Lists, and Indexes"
        table = self.dbf_table
        table.open()
        list = List(table)
        index = Index(table, key=lambda rec: dbf.recno(rec))
        total = len(table)
        mid = total // 2
        self.assertTrue(table[-1] is list[-1])
        self.assertTrue(table[-1] is index[-1])
        self.assertTrue(table.last_record is table[-1])
        self.assertTrue(list.last_record is table[-1])
        self.assertTrue(index.last_record is table[-1])
        table.close()

class Test_Dbf_Lists(unittest.TestCase):
    "DbfList tests"
    def setUp(self):
        "create a dbf table"
        self.dbf_table = table = Table(
            os.path.join(tempdir, 'temptable'),
            'name C(25); paid L; qty N(11,5); orderdate D; desc M', dbf_type='db3'
            )
        table.open()
        for i in range(len(floats)):
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc})
        table.close()
    def tearDown(self):
        self.dbf_table.close()
    def test_exceptions(self):
        table = self.dbf_table
        table.open()
        list = table[::5]
        record = table[5]
        dbf.delete(record)
        self.assertTrue(list[0] is table[0])
        self.assertTrue(record in list)
        self.assertRaises(TypeError, list.__contains__, 'some string')
        self.assertRaises(TypeError, list.__getitem__, 'some string')
        self.assertRaises(TypeError, list.__delitem__, 'some string')
        self.assertRaises(TypeError, list.remove, 'some string')
        self.assertRaises(TypeError, list.index, 'some string')
        self.assertRaises(IndexError, list.__getitem__, 100)
        self.assertRaises(IndexError, list.pop, 1000)
        self.assertRaises(IndexError, list.goto, 1000)
        list.top()
        self.assertRaises(Bof, list.skip, -1)
        list.bottom()
        self.assertRaises(Eof, list.skip)
        table.pack()
        self.assertRaises(DbfError, list.__contains__, record)

        list = List()
        self.assertRaises(IndexError, list.goto, 0)
        self.assertRaises(Bof, list.skip, -1)
        self.assertRaises(Eof, list.skip)
        self.assertRaises(ValueError, list.remove, table[0])
        self.assertRaises(ValueError, list.index, table[1])

    def test_add_subtract(self):
        "addition and subtraction"
        table1 = self.dbf_table
        table1.open()
        list1 = table1[::2]
        list2 = table1[::3]
        list3 = table1[:] - list1 - list2
        self.assertEqual(100, len(table1))
        self.assertEqual(list1[0], list2[0])
        self.assertEqual(list1[3], list2[2])
        self.assertEqual(50, len(list1))
        self.assertEqual(34, len(list2))
        self.assertEqual(33, len(list3))
        self.assertEqual(117, len(list1) + len(list2) + len(list3))
        self.assertEqual(len(table1), len(list1 + list2 + list3))
        self.assertEqual(67, len(list1 + list2))
        self.assertEqual(33, len(list1 - list2))
        self.assertEqual(17, len(list2 - list1))
        table1.close()
    def test_append_extend(self):
        "appending and extending"
        table1 = self.dbf_table
        table1.open()
        list1 = table1[::2]
        list2 = table1[::3]
        list3 = table1[:] - list1 - list2
        list1.extend(list2)
        list2.append(table1[1])
        self.assertEqual(67, len(list1))
        self.assertEqual(35, len(list2))
        list1.append(table1[1])
        list2.extend(list3)
        self.assertEqual(68, len(list1))
        self.assertEqual(67, len(list2))
        table1.close()
    def test_index(self):
        "indexing"
        table1 = self.dbf_table
        table1.open()
        list1 = table1[::2]
        list2 = table1[::3]
        list3 = table1[:] - list1 - list2
        for i, rec in enumerate(list1):
            self.assertEqual(i, list1.index(rec))
        for rec in list3:
            self.assertRaises(ValueError, list1.index, rec )
        table1.close()
    def test_sort(self):
        "sorting"
        table1 = self.dbf_table
        table1.open()
        list1 = table1[::2]
        list2 = table1[::3]
        list3 = table1[:] - list1 - list2
        list4 = table1[:]
        index = table1.create_index(key = lambda rec: rec.name )
        list4.sort(key=lambda rec: rec.name)
        for trec, lrec in zip(index, list4):
            self.assertEqual(dbf.recno(trec), dbf.recno(lrec))
        table1.close()
    def test_keys(self):
        "keys"
        table1 = self.dbf_table
        table1.open()
        field = table1.field_names[0]
        list1 = List(table1, key=lambda rec: rec[field])
        unique = set()
        for rec in table1:
            if rec[field] not in unique:
                unique.add(rec[field])
            else:
                self.assertRaises(NotFoundError, list1.index, rec)
                self.assertFalse(rec in list1)
            self.assertTrue(rec[field] in unique)
        self.assertEqual(len(unique), len(list1))
        table1.close()
    def test_contains(self):
        table = self.dbf_table
        table.open()
        list = List(table)
        i = 0
        for record in list:
            self.assertEqual(record, list[i])
            self.assertTrue(record in list)
            self.assertTrue(tuple(record) in list)
            self.assertTrue(scatter(record) in list)
            self.assertTrue(create_template(record) in list)
            i += 1
        self.assertEqual(i, len(list))
        table.close()

class TestWhatever(unittest.TestCase):
    "move tests here to run one at a time while debugging"
    def setUp(self):
        "create a dbf and vfp table"
        self.dbf_table = table = Table(
            os.path.join(tempdir, 'temptable'),
            'name C(25); paid L; qty N(11,5); orderdate D; desc M', dbf_type='db3'
            )
        table.open()
        namelist = self.dbf_namelist = []
        paidlist = self.dbf_paidlist = []
        qtylist = self.dbf_qtylist = []
        orderlist = self.dbf_orderlist = []
        desclist = self.dbf_desclist = []
        for i in range(len(floats)):
            name = '%-25s' % words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            namelist.append(name)
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(desc)
            table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc})
        table.close()

        self.vfp_table = table = Table(
                os.path.join(tempdir, 'tempvfp'),
                'name C(25); paid L; qty N(11,5); orderdate D; desc M; mass B;'
                ' weight F(18,3); age I; meeting T; misc G; photo P',
                dbf_type='vfp',
                )
        table.open()
        namelist = self.vfp_namelist = []
        paidlist = self.vfp_paidlist = []
        qtylist = self.vfp_qtylist = []
        orderlist = self.vfp_orderlist = []
        desclist = self.vfp_desclist = []
        masslist = self.vfp_masslist = []
        weightlist = self.vfp_weightlist = []
        agelist = self.vfp_agelist = []
        meetlist = self.vfp_meetlist = []
        misclist = self.vfp_misclist = []
        photolist = self.vfp_photolist = []
        for i in range(len(floats)):
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            mass = floats[i] * floats[i] / 2.0
            weight = floats[i] * 3
            age = numbers[i]
            name = words[i]
            paid = len(words[i]) % 3 == 0
            qty = floats[i]
            orderdate = datetime.date((numbers[i] + 1) * 2, (numbers[i] % 12) +1, (numbers[i] % 27) + 1)
            desc = ' '.join(words[i:i+50])
            mass = floats[i] * floats[i] / 2.0
            weight = floats[i] * 3
            age = numbers[i]
            meeting = datetime.datetime((numbers[i] + 2000), (numbers[i] % 12)+1, (numbers[i] % 28)+1, \
                      (numbers[i] % 24), numbers[i] % 60, (numbers[i] * 3) % 60)
            misc = ' '.join(words[i:i+50:3])
            photo = ' '.join(words[i:i+50:7])
            namelist.append('%-25s' % name)
            paidlist.append(paid)
            qtylist.append(qty)
            orderlist.append(orderdate)
            desclist.append(desc)
            masslist.append(mass)
            weightlist.append(weight)
            agelist.append(age)
            meetlist.append(meeting)
            misclist.append(misc)
            photolist.append(photo)
            meeting = datetime.datetime((numbers[i] + 2000), (numbers[i] % 12)+1, (numbers[i] % 28)+1,
                      (numbers[i] % 24), numbers[i] % 60, (numbers[i] * 3) % 60)
            misc = ' '.join(words[i:i+50:3])
            photo = ' '.join(words[i:i+50:7])
            table.append({'name':name, 'paid':paid, 'qty':qty, 'orderdate':orderdate, 'desc':desc,
                    'mass':mass, 'weight':weight, 'age':age, 'meeting':meeting, 'misc':misc, 'photo':photo})
        table.close()
    def tearDown(self):
        self.dbf_table.close()
        self.vfp_table.close()
# main
if __name__ == '__main__':
    tempdir = tempfile.mkdtemp()
    unittest.main()
    shutil.rmtree(tempdir, True)
