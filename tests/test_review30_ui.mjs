import {test} from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {loadApp} from './load_app.mjs';

test('diversion evidence changes the map explanation', () => {
  const app = loadApp();
  const cell = {sample_sufficient:true, journeys:40, distinct_days:5,
    median_gained_secs:180,traversals:40,excluded_by:{'road-closure':40}};
  const html = app.delayStretchHtml({id:'x',from_name:'A',to_name:'B'},cell,
    {journeys:30,distinct_days:5},{hint:'Weekdays'});
  assert.match(html,/40.*road-closure/s);
});

test('a long daytime gap prevents a frequent-service claim', () => {
  const app = loadApp();
  const freq = app.jtFrequency({source:'recorded',series:[{dayType:'weekday',
    points:[480,490,1020,1030].map(m=>({departSecs:m*60}))}]},'any');
  assert.notEqual(freq.level,'plenty');
  assert.match(app.jtFrequencyLabel(freq),/gap|irregular/i);
});

test('passenger defaults exclude uncertain identities and flagged measurements', () => {
  const app=loadApp();
  const rows=[{start:'08:00',estimated:false,match:'declared',qualityFlags:[]},
    {start:'08:00',estimated:false,match:'inferred',qualityFlags:[]},
    {start:'08:00',estimated:false,match:'declared',qualityFlags:['unbounded']}];
  assert.equal(app.journeyTimesFilter(rows,vm.runInContext('JT_DEFAULT_FILTERS',app)).length,1);
});

test('Simple keeps measurement detail in a closed disclosure with a short date range', () => {
  const app = loadApp();
  const html = app.jtMeasurementHtml({dates:['2026-09-20','2026-09-30'],
    scheduled:20,tracked:15,measured:12,eligible:10});
  assert.match(html, /<details class="jt-how"><summary>How we measure<\/summary>/);
  assert.doesNotMatch(html, /\bopen[ =>]|2026-09-/);
  assert.match(html, /20 Sept? to 30 Sept?/);
  assert.match(html, /20 scheduled → 15 tracked/);
  assert.match(html, /left the stop more than 5 min after its\s+timetabled time there/);
  assert.match(html, /can overstate lateness/);
  const answer = app.jtEarlierBusHtml({verdict:'sometimes',before:'ok',journeys:20,late:5,checked:4,madeIt:4,gapSecs:720,lateSecs:300,estimatedDue:0}, 'Churchill Square');
  assert.match(answer, /If you must be there on time, catch the bus before/);
  assert.match(answer, /5 of the 20 buses we timed left Churchill Square/);
  assert.doesNotMatch(answer, /Allow|9 in 10/);
});

test('a superseded planner response cannot change the latest destination result', async () => {
  const app=loadApp(), elements=new Map();
  for(const id of ['plan-journey','plan-form','plan-from','plan-to','plan-stops','plan-results','plan-here'])
    elements.set(id,{value:'',innerHTML:'',handlers:{},addEventListener(n,fn){this.handlers[n]=fn;},focus(){}});
  app.document.getElementById=id=>elements.get(id);
  app.planStopChoices=()=>new Map([['A',{lat:50.8,lon:-.3}],['B',{lat:50.81,lon:-.2}],['C',{lat:50.82,lon:-.1}]]);
  app.track=()=>{};app.planResultsHtml=data=>data.label;
  const pending=[];
  app.apiFetch=()=>new Promise(resolve=>pending.push(resolve));
  app.initJourneyPlanner();
  elements.get('plan-from').value='A'; elements.get('plan-to').value='B';
  const first=elements.get('plan-form').handlers.submit({preventDefault(){}});
  elements.get('plan-to').value='C';
  const second=elements.get('plan-form').handlers.submit({preventDefault(){}});
  pending[1]({label:'A to C'});await second;
  pending[0]({label:'A to B'});await first;
  assert.equal(elements.get('plan-results').innerHTML,'A to C');
});
